import argparse,json,sys,time,uuid,traceback
from pathlib import Path
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'vendor'))
from collab_lab.common import Evidence,write_json
from .adapter import NodeProviders as Providers
from collab_lab.runtime import TaskLedger,RepetitionGuard
from collab_lab.rag import RagLibrary
from .client import Client
from .catalog import CAT,SYSTEM

def failure_identity(folder):
    for line in (folder/'events.jsonl').read_text(encoding='utf-8').splitlines():
        row=json.loads(line)
        if row.get('kind')=='node_identity':return row['data']
    raise RuntimeError('node identity event missing; preserve failure for explicit recovery')

def run_job(args,client,job):
    cid=job['group_task_id'];node=args.node;boot=args.boot
    folder=args.root/'runs'/cid/node
    if job.get('activation',1)>1:folder=folder/('activation_'+str(job['activation']))
    ev=Evidence(folder);ledger=TaskLedger(folder,ev);ledger.enqueue({'group_task_id':cid,'activation':job.get('activation',1),**job['goal']});ledger.activate_next()
    ev.add('node_identity',node,{'hostname':__import__('socket').gethostname(),'node_id':node,'boot_id':boot,'group_task_id':cid,'task':ledger.identity()})
    base={'case_id':cid,'boot_id':boot}
    request_prefix=boot+':'+cid+':activation:'+str(job.get('activation',1))
    r=client.call({'op':'register',**base});
    if not r.get('registered'):raise RuntimeError(r)
    config={'models':{'edge':{'model':'gemma4-e2b:latest','base_url':args.model_url}},'edge_context':65536,'edge_output_tokens':16384,'edge_output_tokens_max':32768,'edge_length_retry':True,'edge_thinking':True,'edge_request_timeout_seconds':900,'request_timeout_seconds':180,'response_retry_attempts':1,'transport_retry_attempts':3,'transport_retry_delays':[1,3]}
    provider=Providers(config);guard=RepetitionGuard(20)
    library=RagLibrary(args.root/'vendor/knowledge',strategy_pack=args.root/'vendor/strategies')
    instructions=SYSTEM+'\n你的节点身份：'+node+'\n工具目录：'+json.dumps({k:{'description':v[0],'input_schema':v[1]} for k,v in CAT.items()},ensure_ascii=False)
    initial={'group_task_id':cid,'node_id':node,'goal':job['goal'],'cloud_available':False}
    messages=[{'role':'system','content':instructions},{'role':'user','content':json.dumps(initial,ensure_ascii=False)}]
    cursor=0;lastobs=None;started=time.monotonic();status='budget_ended';template_seen=False;guard_context={}
    for step in range(job.get('remaining_decisions',64)):
        if time.monotonic()-started>job.get('remaining_seconds',3600):status='observation_window_ended';break
        inbox=client.call({'op':'tool',**base,'name':'read_peer_events','arguments':{'after_sequence':cursor},'request_id':request_prefix+':inbox:'+str(step)})
        if inbox.get('ok') and inbox['result']['messages']:
            rows=inbox['result']['messages'];cursor=inbox['result']['cursor']
            ev.add('peer_messages_delivered_to_model',node,{'message_ids':[x['message_id'] for x in rows],'messages':rows})
            messages.append({'role':'user','content':json.dumps({'peer_events':rows},ensure_ascii=False)})
        # Retain original goal plus a bounded recent history; full raw calls remain on disk.
        model_messages=messages[:2]+messages[2:][-32:]
        answer=provider.call('edge',model_messages,{k:v[1] for k,v in CAT.items()},ev)
        value=answer['parsed'];action=value['action'];arguments=dict(value['arguments']);key=guard.before(action,arguments,guard_context)
        client.call({'op':'trace',**base,'call_id':answer['call_id'],'action':action,'hostname':__import__('socket').gethostname(),'input_sha256':__import__('hashlib').sha256(json.dumps(model_messages,sort_keys=True,ensure_ascii=False).encode()).hexdigest(),'peer_message_ids':[x['message_id'] for row in model_messages if row['role']=='user' for x in json.loads(row['content']).get('peer_events',[])]})
        if action in ('record_finding','configure_local_rate','propose_group_plan','respond_group_plan','prepare_local_plan','apply_local_plan','finish'):
            arguments['observation_id']=lastobs
        if action in ('read_peer_events','wait_peer_event') and 'after_sequence' not in arguments:arguments['after_sequence']=cursor
        ev.add('framework_identity_binding',node,{'model_arguments':value['arguments'],'effective_arguments':arguments,'business_values_rewritten':False})
        rid=request_prefix+':decision:'+str(step)
        if action=='retrieve_knowledge':result={'ok':True,'result':library.search(arguments['query'])}
        elif action=='get_report_template':
            template_seen=True;result={'ok':True,'result':json.loads((args.root/'vendor/templates/capability_gap_report.json').read_text(encoding='utf-8-sig'))}
        elif action=='report_gap':
            ev.add('original_gap_report',node,{'report':arguments['report'],'template_requested':template_seen})
            result={'ok':False,'error_code':'CLOUD_UNAVAILABLE','error':'Cloud support is not available for this run; report saved unchanged.'}
        elif action=='get_request_status':result=client.call({'op':'request_status',**base,'request_id':arguments['request_id']})
        else:
            if action=='wait_peer_event':time.sleep(10)
            result=client.call({'op':'tool',**base,'name':action,'arguments':arguments,'request_id':rid})
        guard.outcome(key,result.get('ok',False));ev.add('node_tool_result',node,{'action':action,'arguments':arguments,'request_id':rid,'response':result})
        if action=='read_sensor_adapter' and result.get('ok'):
            lastobs=result['result']['observation_id']
            guard_context={k:result['result'][k] for k in ('config_version','environment_revision','selected_path','video_rate_mbps','ports','measurements')}
        if action in ('read_peer_events','wait_peer_event') and result.get('ok'):
            cursor=max(cursor,result['result']['cursor'])
            if result['result']['messages']:ev.add('peer_messages_delivered_to_model',node,{'message_ids':[x['message_id'] for x in result['result']['messages']],'messages':result['result']['messages']})
        messages.extend([{'role':'assistant','content':answer['content']},{'role':'user','content':json.dumps({'tool_result':result},ensure_ascii=False)}])
        write_json(folder/'progress.json',{'steps':step+1,'last_action':action,'last_observation_id':lastobs,'status':'running'})
        if action=='finish' and result.get('ok'):
            status=arguments['status'];ledger.transition('solved' if status=='solved' else 'unresolved',{'model_submission':arguments});break
    if status not in ('solved','unresolved','needs_resources'):client.call({'op':'runtime_end',**base,'reason':status})
    write_json(folder/'result.json',{'status':status,'elapsed_seconds':time.monotonic()-started,'model_calls':provider.counter,'node_id':node})

def main():
    p=argparse.ArgumentParser();p.add_argument('--root',type=Path,default=ROOT);p.add_argument('--node',choices=['A','B'],required=True);p.add_argument('--url',default='http://127.0.0.1:18933');p.add_argument('--model-url',required=True);a=p.parse_args()
    a.boot=uuid.uuid4().hex
    auth=json.loads((a.root/'auth.local.json').read_text());client=Client(a.url,auth[a.node]);seen=set()
    while True:
        assignment=client.call({'op':'job'})
        if assignment.get('stop'):break
        job=assignment.get('job')
        key=(job['group_task_id'],job.get('activation',1)) if job else None
        if not job or key in seen:time.sleep(2);continue
        seen.add(key)
        try:run_job(a,client,job)
        except Exception as e:
            folder=a.root/'runs'/job['group_task_id']/a.node
            if job.get('activation',1)>1:folder=folder/('activation_'+str(job['activation']))
            write_json(folder/'failure.json',{'type':type(e).__name__,'error':str(e),'traceback':traceback.format_exc()})
            # The registered boot ID is kept in the first local identity event.
            identity=failure_identity(folder)
            category='model_response_failure' if getattr(e,'code','') in ('MODEL_RESPONSE_INVALID','MODEL_RESPONSE_INCOMPLETE') else type(e).__name__
            client.call({'op':'runtime_end','case_id':job['group_task_id'],'boot_id':identity['boot_id'],'reason':category+': '+str(e)})
if __name__=='__main__':main()
