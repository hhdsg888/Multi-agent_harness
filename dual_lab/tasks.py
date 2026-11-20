"""Receipt-driven local task graph. No diagnosis, parameter choice or tool action.

Only node-visible protocol state, this node's receipts and delivered messages
enter scheduling. The observer's fixture, measurements and peer private state do
not enter this module. The graph is persisted inside the plant's SQLite state.
"""
import copy,time
from .core import require,sha

TERMINAL={'completed','failed','superseded','closed_by_model_exit'}
REPEAT_LIMIT=3
TASKS={
 'OBSERVE_LOCAL':('取得本端当前有效观测，再处理后续职责。',['read_sensor_adapter']),
 'RESPOND_RESOURCE_REQUEST':('回应实际收到的资源请求；自主发布本端观测或明确报告无法回应。',['publish_resource_snapshot','send_peer_message','finish']),
 'PUBLISH_LOCAL_RESOURCES':('发布本端已观测资源，供已发起的协作交换使用。',['publish_resource_snapshot','finish']),
 'ANALYZE_OR_PROPOSE':('依据观测自主调查、选择本地处置或协作方案，也可明确结束。',['record_finding','calculate_link_options','configure_local_rate','request_peer_resources','offer_group_plan','revise_group_plan','send_peer_message','finish']),
 'REVIEW_CURRENT_PLAN':('独立审议当前版本，选择接受并准备、拒绝或明确退出。',['review_group_plan','read_group_plan','finish']),
 'EXECUTE_LOCAL_FRAGMENT':('处理已接受方案的本端执行职责；工具调用仍由本端自主决定。',['execute_group_plan','read_group_plan','finish']),
 'WAIT_PEER_RESOURCES':('等待已请求的对端资源回执。',[]),
 'WAIT_CURRENT_PLAN':('等待当前方案的对端审议回执。',[]),
 'WAIT_AUTHORIZATION':('等待本端已接受方案的授权。',[]),
 'WAIT_BOTH_EXECUTIONS':('等待双方本端执行的真实回执，再开始共同方案核验。',[]),
 'VERIFY_LOCAL_AFTER_BARRIER':('双方执行回执已确认，主动复采本端业务事实。',['read_sensor_adapter']),
 'VERIFY_LOCAL_AFTER_WRITE':('本端配置已写入，主动复采核验。',['read_sensor_adapter']),
 'CLOSE_LOCAL_TASK':('依据实际观测报告solved、unresolved或needs_resources。',['finish','record_finding']),
 'RESOLVE_DECLINED_PLAN':('当前方案被拒绝；自主补充资源事实、提出修改建议、修订或明确退出。',['publish_resource_snapshot','request_peer_resources','revise_group_plan','send_peer_message','wait_for_collaboration','finish']),
 'RESOLVE_BLOCKED_TASK':('真实依赖等待已超时或无进展；依据回执沟通、调查或明确退出。',['read_sensor_adapter','send_peer_message','report_gap','finish']),
 'RECOVER_COORDINATION':('控制信道当前不可用；核对已知本端承诺，自主报告或明确退出。',['read_sensor_adapter','report_gap','finish']),
}

class TaskGraph:
    def __init__(self,plant,case,node):
        self.p,self.c,self.n=plant,case,node
        self.d=case.setdefault('task_graphs',{}).setdefault(node,{
            'parent_goal_id':case['id'],'node_id':node,'activation':0,'tasks':{},
            'current_task_id':None,'context_revision':0,'context_digest':None,
            'barriers':{},'last_observation_task_id':None,'last_local_write':None,
            'wait_failures':{},'runtime_ends':[],'receipt_ids':[]})
    def event(self,kind,data):
        self.p.event(kind,self.n,{'case_id':self.c['id'],**data})
    def complete(self,row,kind,ids):
        if row['state'] in TERMINAL:return
        row.update(state='completed',completion_receipt_type=kind,completion_receipt_ids=list(ids),completed_at=time.time())
        self.event('subtask_completed',{'task_id':row['task_id'],'task_type':row['task_type'],'receipt_type':kind,'receipt_ids':list(ids)})
    def barrier(self,state):
        plan=state.get('current_plan');local=state['local_ledger']['commitments']
        row=next((r for r in reversed(local) if r['local_applied'] and not r['superseded_by_known']),None)
        if not row or plan and plan['plan_id']!=row['plan_id']:return None
        pid=row['plan_id'];known={self.n};evidence=[]
        if plan and plan['plan_id']==pid and plan['plan_hash']==row['plan_hash']:
            known.update(plan['applied']);evidence.append({'source':'authenticated_shared_control_read','plan_id':pid,'plan_hash':plan['plan_hash'],'applied':list(plan['applied'])})
        for message in self.p.received(self.c,self.n):
            payload=message['payload']
            if message['kind']=='execution_receipt' and payload.get('plan_id')==pid and payload.get('plan_hash')==row['plan_hash'] and payload.get('applied') is True:
                known.add(message['sender']);evidence.append({'source':'received_execution_receipt','message_id':message['message_id']})
        saved=self.d['barriers'].get(pid)
        if not saved and known=={'A','B'}:
            saved={'plan_id':pid,'plan_hash':row['plan_hash'],'opened_at':time.time(),'evidence':evidence}
            self.d['barriers'][pid]=saved;self.event('execution_barrier_opened',saved)
        return {'plan_id':pid,'plan_hash':row['plan_hash'],'known_applied_nodes':sorted(known),'confirmed':saved is not None,'receipt_evidence':copy.deepcopy(saved or evidence)}
    def sync(self,state,observation_id):
        activation=self.c['activations'][self.n]
        if activation!=self.d['activation']:
            for row in self.d['tasks'].values():
                if row['state'] not in TERMINAL:row.update(state='superseded',failure_reason='new actual event activation; prior task record retained')
            self.d.update(activation=activation,current_task_id=None)
        plan=state.get('current_plan');pid=state.get('current_plan_id');ob=None
        if state['local_observation_valid']:ob=self.c['observations'][observation_id]
        visible_barrier=self.barrier(state);self.visible_barrier=visible_barrier
        selected=[];keep=set()
        source_ids=[m['message_id'] for m in self.p.received(self.c,self.n)]
        obs_dep=self.d['last_observation_task_id']
        def add(kind,key='',deps=(),waiting=None,complete=None):
            ident='sub_'+sha([self.c['id'],self.n,activation,kind,key])[:16];keep.add(ident)
            row=self.d['tasks'].get(ident)
            if row is None:
                row={'task_id':ident,'parent_goal_id':self.c['id'],'node_id':self.n,'task_type':kind,
                     'activation':activation,'plan_id':pid,'plan_revision':plan.get('plan_revision') if plan else None,
                     'depends_on':[d for d in deps if d],'state':'ready','created_at':time.time(),
                     'source_message_ids':source_ids,'completion_receipt_ids':[],
                     'attempt_budget':{'same_action_error_limit':REPEAT_LIMIT,'global_decisions':64},
                     'deadline':self.c['created']+3600,'failures':{},'wait_target':waiting}
                self.d['tasks'][ident]=row;self.event('subtask_created',{k:row[k] for k in ('task_id','task_type','depends_on','activation')})
            if complete:self.complete(row,*complete)
            if row['state'] not in TERMINAL:
                row['depends_on']=[d for d in deps if d]
                satisfied=all(self.d['tasks'][d]['state']=='completed' for d in row['depends_on'])
                row['state']='waiting' if waiting and satisfied else 'ready' if satisfied else 'blocked'
                if row['state'] in ('ready','waiting'):selected.append(row)
            return row
        own_applied=visible_barrier is not None
        if own_applied:
            barrier=visible_barrier;pid=barrier['plan_id']
            if not barrier['confirmed']:
                add('WAIT_BOTH_EXECUTIONS',pid+':'+sha(source_ids),waiting='execution_receipt')
            else:
                gate=add('WAIT_BOTH_EXECUTIONS',pid+':confirmed',complete=('both_execution_receipts',[sha(self.d['barriers'][pid])]))
                post=ob is not None and ob['sampled_at']>=self.d['barriers'][pid]['opened_at']
                verify=add('VERIFY_LOCAL_AFTER_BARRIER',pid,deps=[gate['task_id']],complete=('post_barrier_observation',[observation_id]) if post else None)
                if post:add('CLOSE_LOCAL_TASK',pid+':'+observation_id,deps=[verify['task_id']])
        elif self.d['last_local_write']:
            write=self.d['last_local_write'];post=ob is not None and ob['sampled_at']>=write['at']
            verify=add('VERIFY_LOCAL_AFTER_WRITE',write['request_id'],complete=('post_write_observation',[observation_id]) if post else None)
            if post:add('CLOSE_LOCAL_TASK',write['request_id']+':'+observation_id,deps=[verify['task_id']])
        else:
            if ob is None:
                observe=add('OBSERVE_LOCAL',str(observation_id));obs_dep=observe['task_id']
            # Commitments have priority over fresh optional resource requests.
            execution='execute_group_plan' in state['available_tools'] or bool(state['local_ledger']['pending_local_execution_plan_ids'])
            if execution:
                add('EXECUTE_LOCAL_FRAGMENT',pid or ':'.join(state['local_ledger']['pending_local_execution_plan_ids']),deps=[obs_dep])
            elif state['pending_resource_request_ids']:
                add('RESPOND_RESOURCE_REQUEST',sha(state['pending_resource_request_ids']),deps=[obs_dep])
            elif plan and not plan['authorized'] and (self.n not in plan['accepted'] or plan['accepted'].get(self.n) is True and not state['local_preparation_valid']):
                add('REVIEW_CURRENT_PLAN',pid+':'+str(observation_id),deps=[obs_dep])
            elif not state['channel_available']:
                add('RECOVER_COORDINATION',pid or '',deps=[obs_dep])
            elif plan and not plan['authorized'] and any(v is False for v in plan['accepted'].values()):
                add('RESOLVE_DECLINED_PLAN',pid,deps=[obs_dep])
            elif (state['outgoing_resource_request_ids'] or plan and not plan['authorized']) and not state['own_resource_snapshot_current']:
                add('PUBLISH_LOCAL_RESOURCES',str(observation_id),deps=[obs_dep])
            elif state['outgoing_resource_request_ids']:
                add('WAIT_PEER_RESOURCES',sha([state['outgoing_resource_request_ids'],source_ids]),deps=[obs_dep],waiting='peer_resources')
            elif plan and not plan['authorized']:
                if any(v is False for v in plan['accepted'].values()):
                    add('RESOLVE_DECLINED_PLAN',pid,deps=[obs_dep])
                else:add('WAIT_CURRENT_PLAN' if self.n=='A' else 'WAIT_AUTHORIZATION',pid+':'+sha(source_ids),deps=[obs_dep],waiting='plan_update' if self.n=='A' else 'authorization')
            else:add('ANALYZE_OR_PROPOSE',sha([observation_id,pid,source_ids]),deps=[obs_dep])
        current=selected[0] if selected else None
        if current is None:
            failed_wait=next((r for ident,r in self.d['tasks'].items() if ident in keep and ident in self.d['wait_failures']),None)
            if failed_wait:
                current=add('RESOLVE_BLOCKED_TASK',failed_wait['task_id']+':'+sha(self.d['wait_failures'][failed_wait['task_id']]))
        if current and current['state']=='waiting' and current['task_id'] in self.d['wait_failures']:
            blocked=current
            current=add('RESOLVE_BLOCKED_TASK',blocked['task_id']+':'+sha(self.d['wait_failures'][blocked['task_id']]))
        for ident,row in self.d['tasks'].items():
            if ident not in keep and row['state'] not in TERMINAL:
                if plan and row['plan_id'] and row['plan_id']!=plan['plan_id']:
                    row.update(state='superseded',failure_reason='new visible plan revision; no completion inferred')
                else:row['state']='queued'  # temporarily preempted, not completed or discarded
        self.d['current_task_id']=current['task_id'] if current else None
        digest=sha({'state':state,'observation_id':observation_id,'activation':activation,'task':current,'wait_failures':self.d['wait_failures']})
        if digest!=self.d['context_digest']:
            self.d['context_revision']+=1;self.d['context_digest']=digest
        card=self.card(current) if current else None
        self.p.save()
        return {'current_task':card,'context_revision':self.d['context_revision'],'context_token':digest,
                'execution_barrier':visible_barrier,'shared_budget':{'remaining_decisions':max(0,64-self.c['decisions'][self.n]),'deadline':self.c['created']+3600},
                'summary':self.summary()}
    def card(self,row):
        card=copy.deepcopy(row);description,tools=TASKS[row['task_type']]
        card.update(instruction=description,focus_tools=tools,context_revision=self.d['context_revision'])
        if row['task_type']=='EXECUTE_LOCAL_FRAGMENT' and row.get('last_error',{}).get('error_code') in ('STALE_OBSERVATION','PREPARE_EXPIRED','NOT_PREPARED'):
            card['instruction']+=' 最近执行被真实前置拒绝；先核对观测和准备版本，自主补采、沟通重审或修订未授权方案。'
            card['focus_tools']=['read_sensor_adapter','read_group_plan','review_group_plan','send_peer_message','revise_group_plan','finish']
        return card
    def check(self,token):
        require(token and token==self.d['context_digest'],'STALE_TASK_CONTEXT','task or visible protocol context changed after input; no automatic retargeting')
        current=self.d['tasks'].get(self.d['current_task_id'])
        require(current and current['state'] not in TERMINAL,'TASK_NOT_ACTIVE','no active subtask; do not regenerate without a new event')
    def record_tool(self,action,args,result,request_id):
        current=self.d['tasks'].get(self.d['current_task_id'])
        if not current:return
        self.d['receipt_ids'].append(request_id)
        reviewed=result.get('result',{}).get('review') if isinstance(result.get('result'),dict) else None
        if result.get('ok') or action=='review_group_plan' and reviewed is not None:
            if action=='read_sensor_adapter':
                kinds={'OBSERVE_LOCAL','VERIFY_LOCAL_AFTER_WRITE','VERIFY_LOCAL_AFTER_BARRIER'}
                if current['task_type'] in kinds:
                    self.complete(current,'sensor_receipt',[request_id,result['result']['observation_id']]);self.d['last_observation_task_id']=current['task_id']
            elif action=='publish_resource_snapshot' and current['task_type'] in ('RESPOND_RESOURCE_REQUEST','PUBLISH_LOCAL_RESOURCES'):
                self.complete(current,'resource_publication',[request_id,result['result']['message_id']])
            elif action=='review_group_plan' and current['task_type']=='REVIEW_CURRENT_PLAN':self.complete(current,'independent_review',[request_id])
            elif action=='execute_group_plan':
                if current['task_type']=='EXECUTE_LOCAL_FRAGMENT':self.complete(current,'local_execution_receipt',[request_id])
            elif current['task_type']=='ANALYZE_OR_PROPOSE' and action in ('request_peer_resources','offer_group_plan','revise_group_plan','configure_local_rate'):
                self.complete(current,'model_selected_branch',[request_id])
            if action=='configure_local_rate':self.d['last_local_write']={'request_id':request_id,'at':time.time()}
            if action in ('offer_group_plan','revise_group_plan'):self.d['last_local_write']=None
            if action=='finish':
                if current['task_type'] in ('CLOSE_LOCAL_TASK','ANALYZE_OR_PROPOSE','RESOLVE_DECLINED_PLAN','RESOLVE_BLOCKED_TASK','RECOVER_COORDINATION'):
                    self.complete(current,'explicit_model_finish',[request_id])
                for row in self.d['tasks'].values():
                    if row['state'] not in TERMINAL:row.update(state='closed_by_model_exit',failure_reason='explicit model exit; dependency not fulfilled')
                self.d['model_submission']=copy.deepcopy(args)
                self.d['model_submission_activation']=self.d['activation']
                self.d.setdefault('model_submissions',[]).append({'activation':self.d['activation'],'arguments':copy.deepcopy(args),'request_id':request_id})
        else:
            current['last_error']={'request_id':request_id,'action':action,'error_code':result.get('error_code')}
            key=sha([action,args,result.get('error_code')]);current['failures'][key]=current['failures'].get(key,0)+1
            if current['failures'][key]>=REPEAT_LIMIT:
                current.update(state='failed',failure_reason='repeated_action_error',last_error_code=result.get('error_code'))
                self.event('subtask_failed',{'task_id':current['task_id'],'reason':'repeated_action_error','action':action,'error_code':result.get('error_code')})
    def end(self,reason):
        current=self.d['tasks'].get(self.d['current_task_id'])
        if current and current['state'] not in TERMINAL:
            current.update(state='failed',failure_reason='failed_no_action' if reason=='model_no_action' else reason)
        self.d['runtime_ends'].append({'activation':self.c['activations'][self.n],'reason':reason,'at':time.time()})
    def wait_result(self,task_id,state,message_ids=()):
        row=self.d['tasks'][task_id]
        if state=='ready':self.complete(row,'actual_peer_event',message_ids)
        elif state in ('timeout','deadlock'):
            self.d['wait_failures'][task_id]={'state':state,'at':time.time()}
            row.update(state='failed',failure_reason='dependency_'+state)
        self.event('scheduler_wait_result',{'task_id':task_id,'state':state,'message_ids':list(message_ids)})
    def summary(self):
        rows=list(self.d['tasks'].values())
        return {'tasks':len(rows),'completed_by_receipt':sum(r['state']=='completed' for r in rows),
                'failed':sum(r['state']=='failed' for r in rows),'superseded':sum(r['state']=='superseded' for r in rows),
                'closed_by_model_exit':sum(r['state']=='closed_by_model_exit' for r in rows),
                'explicit_model_submission':copy.deepcopy(self.d.get('model_submission')) if self.d.get('model_submission_activation')==self.c['activations'][self.n] else None,
                'historical_model_submissions':copy.deepcopy(self.d.get('model_submissions',[])),
                'runtime_ends':copy.deepcopy(self.d['runtime_ends'])}
