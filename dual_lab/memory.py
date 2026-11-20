"""Node-scoped original receipts, never new observations or automatic decisions."""
import copy,hashlib,json,sys
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
if str(ROOT/'vendor') not in sys.path:sys.path.insert(0,str(ROOT/'vendor'))
from collab_lab.common import write_json

def fingerprint(value):
    return hashlib.sha256(json.dumps(value,ensure_ascii=False,sort_keys=True,allow_nan=False).encode('utf-8')).hexdigest()

class WorkingMemoryError(RuntimeError):pass

class WorkingMemory:
    def __init__(self,path,case_id,node_id):
        self.path=Path(path);self.scope={'case_id':case_id,'node_id':node_id}
        self.data={'scope':self.scope,'last_activation':0,'last_observation':None,'last_calculations':{},'last_failure':None,'recent_tools':[],'peer_messages':[],'outcomes':[]}
        if self.path.exists():
            try:
                packet=json.loads(self.path.read_text(encoding='utf-8'))
                if not isinstance(packet,dict) or not isinstance(packet.get('data'),dict):raise ValueError('invalid memory envelope')
                if packet.get('version')!=1 or packet.get('data',{}).get('scope')!=self.scope or packet.get('sha256')!=fingerprint(packet['data']):raise ValueError('scope or checksum mismatch')
                self.data=packet['data']
            except (ValueError,KeyError,TypeError) as error:raise WorkingMemoryError('working memory is invalid; preserve file for review') from error
    def save(self):
        write_json(self.path,{'version':1,'data':self.data,'sha256':fingerprint(self.data)})
    def record_peer(self,rows,activation):
        stored={m['message']['message_id']:m for m in self.data['peer_messages']}
        for message in rows:
            if message.get('group_task_id')!=self.scope['case_id'] or message.get('recipient')!=self.scope['node_id'] or message.get('sender')==self.scope['node_id']:
                raise WorkingMemoryError('received peer message belongs to another scope')
            previous=stored.get(message['message_id'])
            if previous and previous['message']!=message:raise WorkingMemoryError('same peer message ID has different contents')
            if not previous:
                record={'received_in_activation':activation,'message':copy.deepcopy(message)};self.data['peer_messages'].append(record);stored[message['message_id']]=record
        self.data['peer_messages']=self.data['peer_messages'][-12:];self.data['last_activation']=activation;self.save()
    def record_tool(self,action,model_arguments,effective_arguments,response,request_id,activation):
        if action=='read_sensor_adapter' and response.get('ok'):
            observed=response['result']
            if observed.get('node_id')!=self.scope['node_id'] or observed.get('group_task_id')!=self.scope['case_id']:
                raise WorkingMemoryError('local observation belongs to another scope')
        record={'activation':activation,'request_id':request_id,'action':action,'model_arguments':copy.deepcopy(model_arguments),'arguments_at_execution':copy.deepcopy(effective_arguments),'response':copy.deepcopy(response)}
        recent=[r for r in self.data['recent_tools'] if (r['activation'],r['request_id'])!=(activation,request_id)]
        self.data['recent_tools']=(recent+[record])[-12:]
        if action=='read_sensor_adapter' and response.get('ok'):self.data['last_observation']=record
        if action=='calculate_link_options' and response.get('ok'):self.data['last_calculations'][effective_arguments['scope']]=record
        if not response.get('ok'):self.data['last_failure']=record
        self.data['last_activation']=activation;self.save()
    def record_outcome(self,status,activation):
        self.data['outcomes']=[r for r in self.data['outcomes'] if r['activation']!=activation]+[{'activation':activation,'task_end_status':status}]
        self.data['outcomes']=self.data['outcomes'][-8:];self.data['last_activation']=activation;self.save()
    def context(self,max_chars=18000):
        if max_chars<1024:raise ValueError('working-memory context budget is too small')
        if not self.path.exists():return None
        packet=copy.deepcopy(self.data)
        packet.update(source='prior local tool receipts and actually received peer messages; no automatic diagnosis',
                      observation_freshness='historical, not revalidated and not bound to this task; actively read again before writes',
                      persistence_sha256=fingerprint(self.data),omitted_context_records=0)
        # Preserve dedicated last observation/calculation/failure once, rather
        # than repeating the same full receipt in recent history.
        special=[packet['last_observation'],packet['last_failure'],*packet['last_calculations'].values()]
        keys={(r['activation'],r['request_id']) for r in special if r}
        packet['recent_tools']=[r for r in packet['recent_tools'] if (r['activation'],r['request_id']) not in keys]
        def length():return len(json.dumps(packet,ensure_ascii=False,allow_nan=False))
        for name in ('recent_tools','peer_messages','outcomes'):
            while packet[name] and length()>max_chars:packet[name].pop(0);packet['omitted_context_records']+=1
        # Extremely large original payloads stay in the persistent file. Do not
        # invent a condensed observation or business conclusion for the model.
        if length()>max_chars:
            for name in ('last_calculations','last_failure','last_observation'):
                value=packet[name]
                if value:
                    packet[name]={'original_payload_sha256':fingerprint(value),'payload_omitted_due_to_context_budget':True};packet['omitted_context_records']+=1
                if length()<=max_chars:break
        if length()>max_chars:raise WorkingMemoryError('working memory could not fit its declared context budget')
        return packet
