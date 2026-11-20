import copy,hashlib,json,sqlite3,threading,time,uuid
from pathlib import Path
from .feasibility import link_options

def clone(x):return copy.deepcopy(x)
def sha(x):return hashlib.sha256(json.dumps(x,sort_keys=True,ensure_ascii=False,separators=(',',':')).encode()).hexdigest()
class Fault(ValueError):
    def __init__(self,code,detail):self.code=code;super().__init__(detail)
def require(ok,code,detail):
    if not ok:raise Fault(code,detail)

GOAL={'voice_min_satisfaction':.95,'video_min_mbps':2.0,'video_desired_mbps':5.0,'observation_window_per_node':4}
CASES={
 'T00':'正常与小幅允许波动','T01':'A本地速率配置异常','T02':'主连接退化需双端备用配置',
 'T03':'B侧出口资源变化','T04':'B备用容量受限需修订','T05':'准备后B状态变化',
 'T06':'部分执行后节点消息分区','T07':'无可用承载资源','T09':'云端通道不可用但本地能力完整'}

class Plant:
    """Authoritative simulated plant + authenticated mailbox. Never calls an LLM."""
    def __init__(self,path):
        self.path=Path(path);self.path.parent.mkdir(parents=True,exist_ok=True)
        self.db=sqlite3.connect(str(path),check_same_thread=False);self.db.execute('PRAGMA journal_mode=WAL')
        self.db.executescript('CREATE TABLE IF NOT EXISTS state(k TEXT PRIMARY KEY,v TEXT);CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,fp TEXT,result TEXT);CREATE TABLE IF NOT EXISTS events(seq INTEGER PRIMARY KEY AUTOINCREMENT,kind TEXT,actor TEXT,body TEXT);')
        row=self.db.execute("SELECT v FROM state WHERE k='plant'").fetchone()
        self.state=json.loads(row[0]) if row else {'active':None,'cases':{}}
        self.lock=threading.RLock()
    def save(self):
        self.db.execute("INSERT OR REPLACE INTO state VALUES('plant',?)",(json.dumps(self.state),));self.db.commit()
    def event(self,kind,actor,data):
        self.db.execute('INSERT INTO events(kind,actor,body) VALUES(?,?,?)',(kind,actor,json.dumps({'time':time.time(),**data},ensure_ascii=False)))
    def create(self,label,change_policy='after_samples',protocol_mode='legacy',event_reactivation=False,task_dispatch=False,task_revision='r11'):
        require(label in CASES,'UNKNOWN_CASE','unknown fixture')
        require(change_policy in ('after_samples','at_start'),'INVALID_POLICY','unknown change policy')
        require(protocol_mode in ('legacy','structured'),'INVALID_POLICY','unknown protocol mode')
        require(type(event_reactivation) is bool,'INVALID_POLICY','event reactivation must be boolean')
        require(type(task_dispatch) is bool and (not task_dispatch or protocol_mode=='structured'),'INVALID_POLICY','task dispatch requires structured protocol')
        require(task_revision in ('r11','r12','r13','r14'),'INVALID_POLICY','unknown task revision')
        if self.state['active']:
            prior=self.case(self.state['active']);require(self.case_complete(prior),'ACTIVE_CASE','previous case still active or has deliverable follow-up work')
        cid='group_'+uuid.uuid4().hex[:12]
        nodes={n:{'config_version':1,'environment_revision':1,'sample_sequence':0,
                 'selected_path':'primary','video_rate_mbps':5.,'background_mbps':0.,
                 'ports':{'primary':{'physical_up':True,'enabled':True,'channel':'c1','capacity_mbps':8.},
                          'backup':{'physical_up':True,'enabled':False,'channel':'c2','capacity_mbps':6.}}} for n in ('A','B')}
        case={'id':cid,'fixture':label,'protocol_mode':protocol_mode,'event_reactivation':event_reactivation,'task_dispatch':task_dispatch,'task_revision':task_revision,'active_plan_id':None,'change_policy':change_policy,'minimum_samples':2 if change_policy=='at_start' else 4,'nodes':nodes,'observations':{},'messages':[],
              'plans':{},'finished':{'A':None,'B':None},'changed':False,'stale_injected':False,
              'partition':False,'created':time.time(),'findings':[],'executions':[],'boots':{},'peer_delivery':[],
              'activations':{'A':1,'B':1},'node_history':{'A':[],'B':[]},'decisions':{'A':0,'B':0},'delivered':{'A':0,'B':0}}
        self.state['cases'][cid]=case;self.state['active']=cid;self.event('case_created','observer',{'case_id':cid,'fixture':label,'change_policy':change_policy})
        if change_policy=='at_start':self.inject(case)
        self.save();return cid
    def case(self,cid):
        require(cid in self.state['cases'],'UNKNOWN_TASK','unknown task');return self.state['cases'][cid]
    def measure(self,c):
        a,b=c['nodes']['A'],c['nodes']['B'];path=a['selected_path'];capacity=0.
        if b['selected_path']==path:
            pa,pb=a['ports'][path],b['ports'][path]
            if all(p['physical_up'] and p['enabled'] for p in (pa,pb)) and pa['channel']==pb['channel']:
                capacity=max(0.,min(pa['capacity_mbps'],pb['capacity_mbps'])-b['background_mbps'])
        voice=min(.25,capacity);video=min(5.,a['video_rate_mbps'],b['video_rate_mbps'],max(0.,capacity-voice))
        return {'voice_delivered_mbps':voice,'voice_demand_mbps':.25,'video_delivered_mbps':video,'video_demand_mbps':5.}
    def inject(self,c):
        if c['changed'] or (c.get('change_policy','after_samples')=='after_samples' and min(x['sample_sequence'] for x in c['nodes'].values())<2):return
        c['changed']=True;t=c['fixture'];a,b=c['nodes']['A'],c['nodes']['B']
        if t=='T00':a['ports']['primary']['capacity_mbps']=7.8
        elif t=='T01':a['video_rate_mbps']=1.;a['config_version']+=1
        elif t=='T03':b['ports']['primary']['capacity_mbps']=1.
        elif t=='T07':
            for n in (a,b):
                for port in n['ports'].values():port['physical_up']=False
        else:a['ports']['primary']['capacity_mbps']=.8
        if t=='T04':b['ports']['backup']['capacity_mbps']=3.
        for n in (a,b):n['environment_revision']+=1
        self.event('hidden_environment_change','observer',{'case_id':c['id'],'fixture':t})
    def peer(self,c,n,kind,payload):
        require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
        message={'message_id':'msg_'+uuid.uuid4().hex[:12],'sender':n,'recipient':'B' if n=='A' else 'A','kind':kind,'payload':clone(payload),'group_task_id':c['id'],'sequence':len(c['messages'])+1}
        c['messages'].append(message);self.event('message_sent',n,message)
        if kind in ('plan_proposed','plan_response','plan_prepared','plan_prepare_failed','plan_authorized','execution_receipt','resource_snapshot','resource_request','collaboration_request'):self.activate_for_message(c,message['recipient'],message['message_id'])
        return clone(message)
    def improved_tasks(self,c):return c.get('task_dispatch') and c.get('task_revision') in ('r12','r13','r14')
    def pending_activation(self,c,node):
        if not self.improved_tasks(c) or c.get('forced_closed') or c['partition'] or not c.get('event_reactivation'):return None
        finished=c['finished'][node]
        if not finished or not ('model_submission' in finished or finished.get('runtime_status') in ('model_no_action','wait_deadlock','wait_timeout','subtask_repeated_error')):return None
        if c['decisions'][node]>=64 or time.time()-c['created']>=3600:return None
        claimed=set(c.get('activation_message_ids',{}).get(node,[]))
        return next((m for m in self.unread_control_messages(c,node) if m['message_id'] not in claimed),None)
    def case_complete(self,c):
        return all(c['finished'].values()) and not any(self.pending_activation(c,n) for n in ('A','B'))
    def activate_for_message(self,c,target,message_id,on_job_poll=False):
        # r12 queues at send time. A poll sees the final, locked channel state,
        # then returns the source packet itself; send is never mistaken for read.
        if self.improved_tasks(c) and (not on_job_poll or c['partition'] or c.get('forced_closed')):return
        finished=c['finished'][target]
        recoverable=finished and c.get('event_reactivation') and finished.get('runtime_status') in (('model_no_action','wait_deadlock','wait_timeout','subtask_repeated_error') if self.improved_tasks(c) else ('model_no_action','wait_deadlock','wait_timeout'))
        if finished and ('model_submission' in finished or recoverable) and c['decisions'][target]<64 and time.time()-c['created']<3600:
            source=next((m for m in c['messages'] if m['message_id']==message_id and m['recipient']==target and m['kind']!='agent_message'),None)
            if c.get('event_reactivation'):
                claimed=c.setdefault('activation_message_ids',{'A':[],'B':[]})[target]
                if not source or message_id in claimed or message_id in c.get('received_message_ids',{}).get(target,[]):return
                claimed.append(message_id)
                c.setdefault('activation_sources',{})[target]={'source_message_id':message_id,'message_kind':source['kind'],'sender':source['sender']}
                if c.get('waiting'):c['waiting'][target]=None
            c['node_history'][target].append(clone(finished));c['finished'][target]=None;c['activations'][target]+=1
            self.event('peer_task_enqueued',target,{'case_id':c['id'],'activation':c['activations'][target],'source_message_id':message_id})
    def unread_control_messages(self,c,node):
        received=set(c.get('received_message_ids',{}).get(node,[]))
        return [m for m in c['messages'] if m['recipient']==node and m['kind']!='agent_message' and m['message_id'] not in received]
    def obs(self,c,n,ident,allow_prior_activation=False):
        ob=c['observations'].get(ident)
        require(ob and ob['node_id']==n,'OBSERVATION_REQUIRED','reference a real local observation')
        node=c['nodes'][n]
        require(allow_prior_activation or ob.get('activation',1)==c.get('activations',{}).get(n,1),'STALE_OBSERVATION','observation belongs to previous local task activation')
        require(ob['config_version']==node['config_version'] and ob['environment_revision']==node['environment_revision'],'STALE_OBSERVATION','local state changed; actively sample again')
        require(time.time()-ob['sampled_at']<=1800,'STALE_OBSERVATION','observation expired')
        if c.get('task_dispatch') and not allow_prior_activation:
            known={m['message_id'] for m in self.received(c,n) if m['kind']=='execution_receipt'}
            require(known<=set(ob.get('known_peer_execution_ids',[])),'STALE_OBSERVATION','received peer execution changed the known business context; actively sample again')
        return ob
    def plan(self,c,pid):
        require(pid in c['plans'],'UNKNOWN_PLAN','unknown plan_id');return c['plans'][pid]
    def valid_actions(self,c,n,v):
        require(isinstance(v,dict) and set(v)=={'path','channel','video_rate_mbps'},'INVALID_PLAN','use path/channel/video_rate_mbps per node')
        require(v['path'] in ('primary','backup') and v['channel'] in ('c1','c2') and type(v['video_rate_mbps']) in (int,float) and 2<=v['video_rate_mbps']<=5,'INVALID_PLAN','unsupported path/channel or video outside allowed 2..5Mbps')
    def check_ready(self,c,n,p):
        require(not p.get('superseded_by'),'PLAN_SUPERSEDED','plan replaced; read its revision chain')
        require(p['accepted'].get(n) is True,'NOT_ACCEPTED','node has not accepted this plan')
        ready=p['ready'].get(n);require(ready,'NOT_PREPARED','node has not prepared this plan')
        # Accepted reservations survive ordinary local task changes; actual writes
        # still require a new observation in the current activation.
        self.obs(c,n,ready['observation_id'],allow_prior_activation=True)
        require(time.time()<ready['expires_at'],'PREPARE_EXPIRED','reservation expired')
    def public_plan(self,p):return clone(p)
    def received(self,c,n):
        ids=set(c.get('received_message_ids',{}).get(n,[]))
        return [m for m in c['messages'] if m['recipient']==n and m['message_id'] in ids]
    def local_context(self,c,n):
        received=self.received(c,n);rows=[]
        known_auth={m['payload']['plan_id'] for m in received if m['kind']=='plan_authorized'}
        known_auth.update(c.get('seen_authorizations',{}).get(n,[]))
        for pid,p in c['plans'].items():
            if n not in p['accepted']:continue
            authorized=(n=='A' and p['authorized']) or pid in known_auth or n in p['applied']
            superseded=next((q['plan_id'] for q in c['plans'].values() if q.get('replaces_plan_id')==pid and (q['proposer']==n or any(m['kind']=='plan_proposed' and m['payload']['plan_id']==q['plan_id'] for m in received))),None)
            phase='superseded' if superseded else 'applied' if n in p['applied'] else 'declined' if not p['accepted'][n] else 'authorized_execution_pending' if authorized else 'prepared_waiting_authorization' if n in p['ready'] else 'accepted_not_prepared'
            rows.append({'plan_id':pid,'plan_hash':p['plan_hash'],'local_fragment':clone(p['node_actions'][n]),'local_accepted':p['accepted'][n],
                         'local_prepared':n in p['ready'],'authorization_known':bool(authorized),'local_applied':n in p['applied'],'local_phase':phase,
                         'superseded_by_known':superseded,'last_local_execution_attempt':clone(c.get('execution_attempts',{}).get(n,{}).get(pid))})
        return {'node_id':n,'commitments':rows,'pending_local_execution_plan_ids':[r['plan_id'] for r in rows if r['local_phase']=='authorized_execution_pending'],
                'source':'local acceptance/preparation/execution receipts and actually received peer messages; no sensor read or automatic action'}
    def semantic_tool(self,c,n,name,arg):
        """Bundle this actor's bookkeeping. Peer decisions/writes remain separate.

        Outer RPC durably caches the complete receipt, including partial stages.
        """
        stages=[];pid=arg.get('plan_id');current_stage=None
        def stage(action,arguments):
            nonlocal current_stage
            current_stage=action
            value=self.tool(c,n,action,arguments)
            stages.append({'action':action,'result':clone(value)});return value
        try:
            ob={'observation_id':arg.get('observation_id')}
            if name in ('offer_group_plan','revise_group_plan'):
                proposal={**ob,'node_actions':arg['node_actions']}
                if name=='revise_group_plan':proposal['replaces_plan_id']=pid
                p=stage('propose_group_plan',proposal);pid=p['plan_id']
                stage('prepare_local_plan',{**ob,'plan_id':pid})
            elif name=='review_group_plan':
                stage('respond_group_plan',{**ob,'plan_id':pid,'accept':arg['accept'],'explanation':arg.get('explanation','')})
                if arg['accept']:stage('prepare_local_plan',{**ob,'plan_id':pid})
            elif name=='execute_group_plan':
                p=self.plan(c,pid)
                if n=='A' and not p['authorized']:stage('authorize_group_plan',{'plan_id':pid})
                stage('apply_local_plan',{**ob,'plan_id':pid})
            else:raise Fault('UNKNOWN_TOOL',name)
            return {'ok':True,'result':{'plan_id':pid,'stages':stages,'plan':self.public_plan(self.plan(c,pid))}}
        except Fault as e:
            if current_stage=='prepare_local_plan' and pid and not c['partition']:
                failure={'plan_id':pid,'node_id':n,'error_code':e.code,'detail':str(e),'configuration_applied':False}
                if e.code in ('LOCAL_RESOURCE_LIMIT','PHYSICAL_UNAVAILABLE'):
                    observed=c['observations'][arg['observation_id']];fragment=self.plan(c,pid)['node_actions'][n]
                    failure['reported_local_constraints']={'observation_id':observed['observation_id'],'path':fragment['path'],'port':clone(observed['ports'][fragment['path']]),'background_mbps':observed['background_mbps'],'requested_video_rate_mbps':fragment['video_rate_mbps']}
                self.plan(c,pid).setdefault('preparation_failures',{})[n]=clone(failure)
                self.plan(c,pid)['ready'].pop(n,None)
                self.peer(c,n,'plan_prepare_failed',failure)
            return {'ok':False,'error_code':e.code,'error':str(e),'result':{'plan_id':pid,'completed_stages':stages,'partial_progress':bool(stages)}}
        except (KeyError,TypeError,ValueError) as e:
            return {'ok':False,'error_code':'INVALID_ARGUMENTS','error':str(e),'result':{'plan_id':pid,'completed_stages':stages,'partial_progress':bool(stages)}}
    def tool(self,c,n,name,arg):
        node=c['nodes'][n]
        if name=='read_sensor_adapter':
            self.inject(c);node['sample_sequence']+=1
            ob={'observation_id':'obs_'+uuid.uuid4().hex[:12],'node_id':n,'group_task_id':c['id'],'activation':c['activations'][n],'sampled_at':time.time(),**clone(node),'measurements':self.measure(c)}
            if c.get('task_dispatch'):ob['known_peer_execution_ids']=[m['message_id'] for m in self.received(c,n) if m['kind']=='execution_receipt']
            c['observations'][ob['observation_id']]=ob;return ob
        if name=='read_peer_catalog':return {'peer_id':'B' if n=='A' else 'A','capabilities':['local_configuration','group_plan_response','prepare','apply'],'coordinator':'A'}
        if name=='read_local_commitments':return self.local_context(c,n)
        if name=='publish_resource_snapshot':
            ob=self.obs(c,n,arg['observation_id'])
            return self.peer(c,n,'resource_snapshot',{'observation':clone(ob)})
        if name=='calculate_link_options':
            ob=self.obs(c,n,arg['observation_id']);observations={n:ob}
            require(arg['scope'] in ('local','group'),'INVALID_ARGUMENTS','scope must be local or group')
            if arg['scope']=='group':
                snapshots=[m for m in self.received(c,n) if m['kind']=='resource_snapshot']
                require(snapshots,'PEER_RESOURCE_EVIDENCE_REQUIRED','group calculation requires a received authenticated resource snapshot')
                peer_ob=snapshots[-1]['payload']['observation']
                require(time.time()-peer_ob['sampled_at']<=1800,'PEER_RESOURCE_EXPIRED','peer snapshot expired; a new publication is required')
                observations[peer_ob['node_id']]=peer_ob
            return link_options(observations,GOAL)
        if name=='send_peer_message':return self.peer(c,n,'collaboration_request' if arg.get('requires_response') else 'agent_message',arg['payload'])
        if name in ('read_peer_events','wait_peer_event'):
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
            rows=[clone(x) for x in c['messages'] if x['recipient']==n and x['sequence']>arg.get('after_sequence',0)]
            if rows:
                c['delivered'][n]=max(c['delivered'][n],max(x['sequence'] for x in rows))
                seen=c.setdefault('received_message_ids',{'A':[],'B':[]})[n]
                seen.extend(x['message_id'] for x in rows if x['message_id'] not in seen)
                self.event('messages_received',n,{'case_id':c['id'],'message_ids':[x['message_id'] for x in rows]})
            return {'messages':rows,'cursor':len(c['messages'])}
        if name=='record_finding':
            self.obs(c,n,arg['observation_id']);c['findings'].append({'node':n,**clone(arg)});return {'recorded':True}
        if name=='configure_local_rate':
            self.obs(c,n,arg['observation_id']);rate=arg['video_rate_mbps']
            require(type(rate) in (int,float) and 2<=rate<=5,'INVALID_ARGUMENTS','video rate allowed 2..5 Mbps')
            require(rate!=node['video_rate_mbps'],'NO_CHANGE','configuration already has this value')
            node['video_rate_mbps']=rate;node['config_version']+=1
            self.event('local_execute',n,{'case_id':c['id'],'rate':rate});return {'applied':True,'node_id':n,'config_version':node['config_version']}
        if name=='propose_group_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
            self.obs(c,n,arg['observation_id']);changes=arg['node_actions']
            replaces=arg.get('replaces_plan_id')
            if replaces:
                old=self.plan(c,replaces)
                require(not old['authorized'] and not old['applied'],'PLAN_COMMITTED','cannot replace a committed or partially applied plan')
                require(not old.get('superseded_by'),'PLAN_SUPERSEDED','replace the current revision, not an old plan')
            require(set(changes)=={'A','B'},'INVALID_PLAN','supply A and B plan fragments')
            for owner,v in changes.items():self.valid_actions(c,owner,v)
            require(changes['A']['path']==changes['B']['path'] and changes['A']['channel']==changes['B']['channel'],'INCOMPATIBLE_ENDPOINTS','both endpoints require same path and channel')
            pid='plan_'+uuid.uuid4().hex[:10]
            p={'plan_id':pid,'plan_revision':len(c['plans'])+1,'group_task_id':c['id'],'plan_hash':sha(changes),'node_actions':clone(changes),'proposer':n,'accepted':{n:True},'ready':{},'authorized':False,'applied':[]}
            if replaces:
                old['superseded_by']=pid;p['replaces_plan_id']=replaces
                self.event('plan_superseded',n,{'case_id':c['id'],'old_plan_id':replaces,'new_plan_id':pid})
            c['plans'][pid]=p;self.peer(c,n,'plan_proposed',self.public_plan(p));return self.public_plan(p)
        if name=='read_group_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable');p=self.plan(c,arg['plan_id'])
            if p['authorized']:
                seen=c.setdefault('seen_authorizations',{'A':[],'B':[]})[n]
                if p['plan_id'] not in seen:seen.append(p['plan_id'])
            return self.public_plan(p)
        if name=='respond_group_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable');self.obs(c,n,arg['observation_id'])
            p=self.plan(c,arg['plan_id']);require(not p['authorized'],'PLAN_COMMITTED','cannot revise committed decision')
            require(not p.get('superseded_by'),'PLAN_SUPERSEDED','plan replaced; read its revision chain')
            require(type(arg['accept']) is bool,'INVALID_ARGUMENTS','accept must be boolean');p['accepted'][n]=arg['accept'];p['ready'].pop(n,None)
            self.peer(c,n,'plan_response',{'plan_id':p['plan_id'],'accept':arg['accept'],'explanation':arg.get('explanation','')});return self.public_plan(p)
        if name=='prepare_local_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
            self.obs(c,n,arg['observation_id']);p=self.plan(c,arg['plan_id']);require(p['accepted'].get(n) is True,'NOT_ACCEPTED','accept plan first')
            require(not p.get('superseded_by'),'PLAN_SUPERSEDED','plan replaced; read its revision chain')
            v=p['node_actions'][n];port=node['ports'][v['path']]
            require(port['physical_up'],'PHYSICAL_UNAVAILABLE','selected local port is physically unavailable')
            require(.25+v['video_rate_mbps']<=max(0,port['capacity_mbps']-node['background_mbps']),'LOCAL_RESOURCE_LIMIT','requested local reservation exceeds available capacity')
            p['ready'][n]={'node_id':n,'observation_id':arg['observation_id'],'plan_hash':p['plan_hash'],'expires_at':time.time()+1800}
            p.get('preparation_failures',{}).pop(n,None)
            self.peer(c,n,'plan_prepared',{'plan_id':p['plan_id'],'node_id':n,'plan_hash':p['plan_hash']})
            if c['fixture']=='T05' and len(p['ready'])==2 and not c['stale_injected']:
                c['stale_injected']=True;c['nodes']['B']['ports']['backup']['capacity_mbps']=3.;c['nodes']['B']['environment_revision']+=1
                self.event('hidden_prepare_change','observer',{'case_id':c['id']})
            return clone(p['ready'][n])
        if name=='authorize_group_plan':
            require(n=='A','FORBIDDEN','only fixed coordinator A can authorize');require(not c['partition'],'PEER_UNAVAILABLE','message channel unavailable')
            p=self.plan(c,arg['plan_id']);require(not p['authorized'],'PLAN_COMMITTED','plan already authorized')
            for owner in ('A','B'):self.check_ready(c,owner,p)
            p['authorized']=True;self.peer(c,n,'plan_authorized',{'plan_id':p['plan_id'],'plan_hash':p['plan_hash']});return self.public_plan(p)
        if name=='apply_local_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable; committed state must be queried after recovery')
            self.obs(c,n,arg['observation_id']);p=self.plan(c,arg['plan_id']);require(p['authorized'],'NOT_AUTHORIZED','plan has not been authorized')
            require(n not in p['applied'],'ALREADY_APPLIED','query existing receipt, do not repeat application');self.check_ready(c,n,p)
            v=p['node_actions'][n];node.update(selected_path=v['path'],video_rate_mbps=v['video_rate_mbps']);node['ports'][v['path']].update(enabled=True,channel=v['channel']);node['config_version']+=1;p['applied'].append(n)
            receipt={'node_id':n,'plan_id':p['plan_id'],'plan_hash':p['plan_hash'],'config_version':node['config_version'],'applied':True}
            c['executions'].append(receipt);self.peer(c,n,'execution_receipt',receipt);self.event('plan_execute',n,{'case_id':c['id'],**receipt})
            if c['fixture']=='T06' and len(p['applied'])==1:
                c['partition']=True;self.event('hidden_peer_partition','observer',{'case_id':c['id']})
            return receipt
        if name=='finish':
            self.obs(c,n,arg['observation_id'])
            enough=node['sample_sequence']>=2 if c.get('change_policy')=='at_start' else min(x['sample_sequence'] for x in c['nodes'].values())>=4
            if c.get('protocol_mode')!='structured':require(enough,'OBSERVATION_WINDOW','required active observation window has not completed')
            require(arg['status'] in ('solved','unresolved','needs_resources'),'INVALID_ARGUMENTS','unknown finish status')
            c['finished'][n]={'model_submission':clone(arg),'measurements':self.measure(c),'finished_at':time.time()}
            pending=self.unread_control_messages(c,n) if c.get('event_reactivation') else [m for m in c['messages'] if m['recipient']==n and m['sequence']>c['delivered'][n] and m['kind']!='agent_message']
            if pending:self.activate_for_message(c,n,pending[0]['message_id'])
            return {'local_task_ended':True,'model_status':arg['status']}
        raise Fault('UNKNOWN_TOOL',name)
    def rpc(self,actor,data):
        with self.lock:
            op=data.get('op')
            if actor=='admin':
                if op=='capabilities':return {'task_dispatch_revision':'r11','task_dispatch_revisions':['r11','r12','r13','r14'],'stable_public_tools':17,'task_context_binding':True,'post_execution_barrier':True}
                if op=='begin_suite':
                    if self.state['active']:require(self.case_complete(self.case(self.state['active'])),'ACTIVE_CASE','previous suite still has live nodes or deliverable follow-up work')
                    self.state.update(suite_done=False,active=None,suite_name=data['name']);self.save();return {'started':data['name']}
                if op=='suite_end':self.state['suite_done']=True;self.save();return {'stopped':True}
                if op=='create':return {'case_id':self.create(data['fixture'],data.get('change_policy','after_samples'),data.get('protocol_mode','legacy'),data.get('event_reactivation',False),data.get('task_dispatch',False),data.get('task_revision','r11'))}
                if op=='snapshot':
                    c=self.case(data['case_id']);view=clone(c)
                    if self.improved_tasks(c):view['case_complete']=self.case_complete(c)
                    return view
                if op=='force_end':
                    c=self.case(data['case_id'])
                    if self.improved_tasks(c):c['forced_closed']=True
                    for n in ('A','B'):
                        if c['finished'][n] is None:c['finished'][n]={'runtime_status':data['reason']}
                    self.save();return {'ended':True}
                if op=='score':return self.score(self.case(data['case_id']))
                if op=='events':return [{'seq':seq,'kind':k,'actor':a,'body':json.loads(b)} for seq,k,a,b in self.db.execute('SELECT seq,kind,actor,body FROM events')]
                if op=='status':return {'active':self.state['active'],'cases':[{ 'id':c['id'],'fixture':c['fixture'],'finished':c['finished']} for c in self.state['cases'].values()]}
                raise Fault('UNKNOWN_OPERATION',str(op))
            require(actor in ('A','B'),'FORBIDDEN','node not registered')
            if op=='job':
                if self.state.get('suite_done'):return {'job':None,'stop':True}
                cid=self.state['active']
                if not cid:return {'job':None}
                c=self.case(cid)
                source=self.pending_activation(c,actor)
                if source:
                    self.activate_for_message(c,actor,source['message_id'],on_job_poll=True)
                    inbox=self.tool(c,actor,'read_peer_events',{'after_sequence':c['delivered'][actor]})
                    require(any(m['message_id']==source['message_id'] for m in inbox['messages']),'SOURCE_DELIVERY_REQUIRED','new task requires its actual source packet')
                    c.setdefault('activation_inboxes',{})[actor]={'activation':c['activations'][actor],**inbox}
                    self.event('task_source_delivered',actor,{'case_id':cid,'activation':c['activations'][actor],'message_ids':[m['message_id'] for m in inbox['messages']]});self.save()
                if c['finished'][actor]:return {'job':None}
                goal={**GOAL,'observation_window_per_node':c.get('minimum_samples',4)}
                if c.get('protocol_mode')=='structured':goal={**goal,'observation_window_per_node':1,'completion_semantics':'solved means verified minimum service floor; desired quality and protocol completion are separate','voice_satisfaction_definition':'voice_delivered_mbps / voice_demand_mbps'}
                source=c.get('activation_sources',{}).get(actor)
                job={'group_task_id':cid,'node_id':actor,'goal':goal,'cloud_available':False,'protocol_mode':c.get('protocol_mode','legacy'),'task_dispatch':c.get('task_dispatch',False),'task_revision':c.get('task_revision','r11'),'activation':c.get('activations',{}).get(actor,1),'activation_source':{'source_message_id':source['source_message_id']} if source else None,'remaining_decisions':64-c.get('decisions',{}).get(actor,0),'remaining_seconds':max(0,3600-(time.time()-c['created']))}
                inbox=c.get('activation_inboxes',{}).get(actor)
                if self.improved_tasks(c) and inbox and inbox['activation']==job['activation']:job['activation_inbox']=clone(inbox)
                return {'job':job}
            c=self.case(data['case_id'])
            if op=='register':
                boot=data['boot_id'];prior=c['boots'].get(actor)
                require(not prior or prior==boot,'EXPLICIT_RECOVERY_REQUIRED','previous node boot already registered; explicit recovery needed');c['boots'][actor]=boot;self.save();return {'registered':actor}
            require(c['boots'].get(actor)==data.get('boot_id'),'STALE_BOOT','node boot identity mismatch')
            if op=='local_context':return self.local_context(c,actor)
            if op=='task_snapshot':
                require(c.get('task_dispatch'),'INVALID_POLICY','task scheduler not enabled')
                return clone(c.get('task_graphs',{}).get(actor,{}))
            if op=='runtime_state':
                from .protocol import StructuredProtocol
                return StructuredProtocol(self,c,actor).state(data.get('observation_id'))
            if op=='scheduler_wait':
                from .protocol import StructuredProtocol
                from .tasks import TaskGraph
                require(c.get('task_dispatch'),'INVALID_POLICY','task scheduler not enabled')
                state=StructuredProtocol(self,c,actor).state(data.get('observation_id'))['task_dispatch']
                graph=TaskGraph(self,c,actor);graph.check(data.get('task_context_token'))
                task=state['current_task'];require(task['state']=='waiting','WAIT_DEPENDENCY_REQUIRED','scheduler only waits on a real pending dependency')
                require(data['after_sequence']>=c['delivered'][actor],'STALE_CURSOR','do not replay previously delivered messages')
                seconds=data.get('seconds',180);require(type(seconds) in (int,float) and 0<seconds<=180,'INVALID_ARGUMENTS','wait interval must be within 180 seconds')
                c.setdefault('waiting',{'A':None,'B':None})[actor]={'after_sequence':data['after_sequence'],'awaited':task['wait_target'],'scheduler_task_id':task['task_id'],'deadline':min(time.time()+seconds,c['created']+3600)}
                self.event('scheduler_wait_registered',actor,{'case_id':c['id'],**c['waiting'][actor]});self.save()
                return {'state':'waiting','task_id':task['task_id']}
            if op in ('idle','idle_poll'):
                waiting=c.setdefault('waiting',{'A':None,'B':None})
                if c['finished'][actor]:return {'state':'ended'}
                if op=='idle':
                    require(c.get('protocol_mode')!='structured','EXPLICIT_WAIT_REQUIRED','structured tasks require an explicit wait tool choice')
                    waiting[actor]={'after_sequence':data['after_sequence'],'call_id':data['call_id']}
                    self.event('node_waiting',actor,{'case_id':c['id'],**waiting[actor]})
                require(waiting[actor] is not None,'NOT_WAITING','idle entry required')
                cursor=waiting[actor]['after_sequence']
                pending=[m for m in c['messages'] if m['recipient']==actor and m['sequence']>cursor]
                if pending and not c['partition']:
                    intent=clone(waiting[actor]);inbox=self.tool(c,actor,'read_peer_events',{'after_sequence':cursor});waiting[actor]=None
                    self.event('node_woken',actor,{'case_id':c['id'],'message_ids':[m['message_id'] for m in inbox['messages']]})
                    expected={'peer_resources':('resource_snapshot',),'plan_update':('plan_proposed','plan_response'),'authorization':('plan_authorized',),'execution_receipt':('execution_receipt',)}.get(intent.get('awaited'),())
                    if intent.get('scheduler_task_id'):
                        from .tasks import TaskGraph
                        TaskGraph(self,c,actor).wait_result(intent['scheduler_task_id'],'ready',[m['message_id'] for m in inbox['messages']])
                    self.save();return {'state':'ready',**inbox,'awaited':intent.get('awaited'),'awaited_event_observed':any(m['kind'] in expected for m in inbox['messages'])}
                peer='B' if actor=='A' else 'A'
                peer_wait=waiting[peer]
                peer_pending=peer_wait and any(m['recipient']==peer and m['sequence']>peer_wait['after_sequence'] for m in c['messages'])
                quiet=(c['finished'][peer] is not None or peer_wait is not None) and not peer_pending
                if self.pending_activation(c,peer):quiet=False
                terminal='deadlock' if c.get('protocol_mode')=='structured' else 'quiescent'
                if waiting[actor].get('scheduler_task_id') and (quiet or time.time()>=waiting[actor]['deadline']):
                    from .tasks import TaskGraph
                    state='deadlock' if quiet else 'timeout';intent=waiting[actor];waiting[actor]=None
                    TaskGraph(self,c,actor).wait_result(intent['scheduler_task_id'],state)
                    self.save();return {'state':state,'task_id':intent['scheduler_task_id'],'channel_available':not c['partition']}
                self.save();return {'state':terminal if quiet else 'waiting','channel_available':not c['partition']}
            if op=='trace':
                c['decisions'][actor]+=1
                self.event('model_turn',actor,{k:v for k,v in data.items() if k not in ('op','boot_id')});self.save();return {'recorded':True}
            if op=='runtime_end':
                if c.get('task_dispatch'):
                    from .tasks import TaskGraph
                    TaskGraph(self,c,actor).end(data['reason'])
                c['finished'][actor]={'runtime_status':data['reason']}
                if c.get('event_reactivation'):
                    c['finished'][actor]['ended_at']=time.time()
                    pending=self.unread_control_messages(c,actor)
                    if pending:self.activate_for_message(c,actor,pending[0]['message_id'])
                self.save();return {'ended':True}
            if op=='request_status':
                row=self.db.execute('SELECT result FROM requests WHERE id=?',(actor+':'+data['request_id'],)).fetchone();return {'status':'complete' if row else 'unknown','result':json.loads(row[0]) if row else None}
            require(op=='tool','UNKNOWN_OPERATION',str(op));require(not c['finished'][actor],'TASK_ENDED','local task already ended')
            rid=actor+':'+data['request_id'];identity={k:data[k] for k in ('case_id','boot_id','name','arguments')}
            if c.get('task_dispatch'):identity.update(task_context_token=data.get('task_context_token'),model_arguments=data.get('model_arguments'))
            fp=sha(identity)
            row=self.db.execute('SELECT fp,result FROM requests WHERE id=?',(rid,)).fetchone()
            if row:
                require(row[0]==fp,'REQUEST_ID_CONFLICT','same request_id with different operation');return json.loads(row[1])
            require('node_id' not in data['arguments'] or data['arguments']['node_id']==actor,'FORBIDDEN','cannot address another node executor')
            try:
                if c.get('protocol_mode')=='structured':
                    from .protocol import StructuredProtocol
                    result=StructuredProtocol(self,c,actor).dispatch(data['name'],data['arguments'],data.get('task_context_token'),data.get('model_arguments'))
                elif data['name'] in ('offer_group_plan','revise_group_plan','review_group_plan','execute_group_plan'):result=self.semantic_tool(c,actor,data['name'],data['arguments'])
                else:result={'ok':True,'result':self.tool(c,actor,data['name'],data['arguments'])}
            except Fault as e:result={'ok':False,'error_code':e.code,'error':str(e)}
            except (KeyError,TypeError,ValueError) as e:result={'ok':False,'error_code':'INVALID_ARGUMENTS','error':str(e)}
            if c.get('task_dispatch') and data['name']!='read_peer_events':
                from .tasks import TaskGraph
                graph=TaskGraph(self,c,actor)
                if data.get('task_context_token')==graph.d['context_digest']:
                    graph.record_tool(data['name'],data['arguments'] if result.get('ok') else data.get('model_arguments',data['arguments']),result,data['request_id'])
                    current=graph.d['tasks'].get(graph.d['current_task_id'])
                    result['task_dispatch_outcome']={'task_id':graph.d['current_task_id'],'task_failed':bool(current and current['state']=='failed'),'summary':graph.summary()}
            if data['name'] in ('execute_group_plan','apply_local_plan') and data['arguments'].get('plan_id') in c['plans']:
                attempts=c.setdefault('execution_attempts',{'A':{},'B':{}})[actor]
                attempts[data['arguments']['plan_id']]={'request_id':data['request_id'],'ok':result['ok'],'error_code':result.get('error_code')}
            self.event('tool_result',actor,{'case_id':c['id'],'request_id':data['request_id'],'action':data['name'],'arguments':data['arguments'],'response':result})
            self.db.execute('INSERT INTO requests VALUES(?,?,?)',(rid,fp,json.dumps(result)));self.save();return result
    def score(self,c):
        m=self.measure(c);solved=m['voice_delivered_mbps']>=.25*.95 and m['video_delivered_mbps']>=2
        full=[p['plan_id'] for p in c['plans'].values() if set(p['applied'])=={'A','B'}]
        local_writes=sum(json.loads(row[0]).get('case_id')==c['id'] for row in self.db.execute("SELECT body FROM events WHERE kind='local_execute'"))
        model_turns={n:sum(json.loads(row[0]).get('case_id')==c['id'] for row in self.db.execute("SELECT body FROM events WHERE kind='model_turn' AND actor=?",(n,))) for n in ('A','B')}
        return {'case_id':c['id'],'fixture':c['fixture'],'measurements':m,'business_target_met':solved,'full_dual_plan_ids':full,
                'dual_execution_observed':bool(full),'messages_sent':len(c['messages']),'node_results':clone(c['finished']),
                'model_turns':model_turns,'model_findings':clone(c['findings']),'local_rate_writes':local_writes,
                'node_task_history':clone(c.get('node_history',{})),'node_activations':clone(c.get('activations',{})),
                'restoration_verified':c['fixture']!='T00' and solved and bool(full or local_writes),
                'normal_unnecessary_writes':len(c['executions'])+local_writes if c['fixture']=='T00' else None,
                'correct_solved_confirmation':{n:bool(x and x.get('model_submission',{}).get('status')=='solved' and solved) for n,x in c['finished'].items()},
                'changed':c['changed'],'prepare_change_observed':c['stale_injected'],'peer_partition_observed':c['partition'],
                'service_floor_met':solved,'desired_quality_met':solved and m['video_delivered_mbps']>=GOAL['video_desired_mbps'],
                'protocol_status':self.protocol_status(c),'sample_sequences':{n:v['sample_sequence'] for n,v in c['nodes'].items()},
                'sampling_window_compliant':{n:v['sample_sequence']>=2 for n,v in c['nodes'].items()},
                'task_dispatch_enabled':c.get('task_dispatch',False),
                'subtask_summaries':{n:__import__('dual_lab.tasks',fromlist=['TaskGraph']).TaskGraph(self,c,n).summary() for n in ('A','B')} if c.get('task_dispatch') else {}}
    def protocol_status(self,c):
        p=c['plans'].get(c.get('active_plan_id'))
        if p is None:return 'not_started'
        if set(p['applied'])=={'A','B'}:return 'applied_both'
        if p['authorized']:return 'authorized_partial' if p['applied'] else 'authorized'
        if set(p['ready'])=={'A','B'}:return 'prepared'
        if any(v is False for v in p['accepted'].values()):return 'declined'
        return 'negotiating'
