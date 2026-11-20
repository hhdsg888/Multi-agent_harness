"""Native Ollama calls; no text extraction, JSON grammar or action repair."""
import copy,json,time,urllib.request
from collab_lab import providers as transport
from collab_lab.common import dumps,write_json

def parse_native(raw,actions,ident,allow_idle=False):
    if not raw.get('done') or raw.get('done_reason')=='length':
        raise transport.IncompleteResponseError('Native tool response incomplete',raw.get('done_reason'),ident)
    calls=raw.get('message',{}).get('tool_calls',[])
    if not isinstance(calls,list):raise ValueError('Malformed native tool_calls')
    if not calls and allow_idle and raw.get('done_reason')=='stop':return None
    if len(calls)!=1:raise ValueError('Expected exactly one native tool call; no text fallback or partial execution')
    function=calls[0].get('function',{})
    if function.get('name') not in actions or not isinstance(function.get('arguments'),dict):
        raise ValueError('Native call name/arguments do not match public interface')
    return {'action':function['name'],'arguments':copy.deepcopy(function['arguments'])}

class NodeProviders(transport.Providers):
    def _call_once(self,role,messages,actions,evidence,output_tokens=None,attempt=1,retry_of=None):
        if role!='edge':return super()._call_once(role,messages,actions,evidence,output_tokens,attempt,retry_of)
        from .catalog import CAT
        with self.lock:self.counter+=1;ident=f'{self.counter:06d}'
        connection,opener,headers=self._connection(role)
        body={'model':connection['model'],'messages':copy.deepcopy(messages),
              'tools':[{'type':'function','function':{'name':name,'description':CAT[name][0],'parameters':schema}} for name,schema in actions.items()],
              'stream':False,'think':self.config.get('edge_thinking',True),
              'options':{'temperature':0,'num_ctx':self.config['edge_context'],'num_predict':output_tokens}}
        base=evidence.directory/'calls'/role/ident
        write_json(str(base)+'_request.json',body)
        evidence.add('model_request',role,{'call_id':ident,'model':connection['model'],'actions':list(actions),'attempt':attempt,'retry_of':retry_of,'output_limit':output_tokens,'transport':'native_tools'})
        started=time.monotonic();raw=None
        try:
            request=urllib.request.Request(connection['base_url'].rstrip('/')+'/api/chat',data=dumps(body).encode(),headers=headers)
            with opener.open(request,timeout=self.config.get('edge_request_timeout_seconds',900)) as response:raw=json.load(response)
            parsed=parse_native(raw,actions,ident,allow_idle=True)
            result={'parsed':parsed,'control':'idle' if parsed is None else 'tool_call','content':raw['message'].get('content',''),'assistant_message':raw['message'],
                    'raw':raw,'model':connection['model'],'call_id':ident,'seconds':time.monotonic()-started,
                    'finish_reason':raw.get('done_reason'),'attempt':attempt,'retry_of':retry_of,
                    'usage':{'prompt_tokens':raw.get('prompt_eval_count'),'output_tokens':raw.get('eval_count')}}
            write_json(str(base)+'_response.json',result)
            evidence.add('model_response',role,{k:result[k] for k in ('call_id','model','seconds','parsed','finish_reason','usage','attempt','retry_of')})
            return result
        except Exception as error:
            result={'call_id':ident,'error':f'{type(error).__name__}: {error}','content':(raw or {}).get('message',{}).get('content',''),'raw':raw,'seconds':time.monotonic()-started}
            write_json(str(base)+'_error.json',result);evidence.add('model_error',role,{k:v for k,v in result.items() if k!='raw'});raise
