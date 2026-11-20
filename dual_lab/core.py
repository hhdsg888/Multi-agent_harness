import copy,hashlib,json,sqlite3,threading,time,uuid
from pathlib import Path

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
    def create(self,label):
        require(label in CASES,'UNKNOWN_CASE','unknown fixture')
        if self.state['active']:
            prior=self.case(self.state['active']);require(all(prior['finished'].values()),'ACTIVE_CASE','previous case still active')
        cid='group_'+uuid.uuid4().hex[:12]
        nodes={n:{'config_version':1,'environment_revision':1,'sample_sequence':0,
                 'selected_path':'primary','video_rate_mbps':5.,'background_mbps':0.,
                 'ports':{'primary':{'physical_up':True,'enabled':True,'channel':'c1','capacity_mbps':8.},
                          'backup':{'physical_up':True,'enabled':False,'channel':'c2','capacity_mbps':6.}}} for n in ('A','B')}
        case={'id':cid,'fixture':label,'nodes':nodes,'observations':{},'messages':[],
              'plans':{},'finished':{'A':None,'B':None},'changed':False,'stale_injected':False,
              'partition':False,'created':time.time(),'findings':[],'executions':[],'boots':{},'peer_delivery':[],
              'activations':{'A':1,'B':1},'node_history':{'A':[],'B':[]},'decisions':{'A':0,'B':0},'delivered':{'A':0,'B':0}}
        self.state['cases'][cid]=case;self.state['active']=cid;self.event('case_created','observer',{'case_id':cid,'fixture':label});self.save();return cid
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
        if c['changed'] or min(x['sample_sequence'] for x in c['nodes'].values())<2:return
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
        if kind in ('plan_proposed','plan_response','plan_prepared','plan_authorized','execution_receipt','collaboration_request'):self.activate_for_message(c,message['recipient'],message['message_id'])
        return clone(message)
    def activate_for_message(self,c,target,message_id):
        finished=c['finished'][target]
        if finished and 'model_submission' in finished and c['decisions'][target]<64 and time.time()-c['created']<3600:
            c['node_history'][target].append(clone(finished));c['finished'][target]=None;c['activations'][target]+=1
            self.event('peer_task_enqueued',target,{'case_id':c['id'],'activation':c['activations'][target],'source_message_id':message_id})
    def obs(self,c,n,ident):
        ob=c['observations'].get(ident)
        require(ob and ob['node_id']==n,'OBSERVATION_REQUIRED','reference a real local observation')
        node=c['nodes'][n]
        require(ob.get('activation',1)==c.get('activations',{}).get(n,1),'STALE_OBSERVATION','observation belongs to previous local task activation')
        require(ob['config_version']==node['config_version'] and ob['environment_revision']==node['environment_revision'],'STALE_OBSERVATION','local state changed; actively sample again')
        require(time.time()-ob['sampled_at']<=1800,'STALE_OBSERVATION','observation expired');return ob
    def plan(self,c,pid):
        require(pid in c['plans'],'UNKNOWN_PLAN','unknown plan_id');return c['plans'][pid]
    def valid_actions(self,c,n,v):
        require(isinstance(v,dict) and set(v)=={'path','channel','video_rate_mbps'},'INVALID_PLAN','use path/channel/video_rate_mbps per node')
        require(v['path'] in ('primary','backup') and v['channel'] in ('c1','c2') and type(v['video_rate_mbps']) in (int,float) and 2<=v['video_rate_mbps']<=5,'INVALID_PLAN','unsupported path/channel or video outside allowed 2..5Mbps')
    def check_ready(self,c,n,p):
        require(p['accepted'].get(n) is True,'NOT_ACCEPTED','node has not accepted this plan')
        ready=p['ready'].get(n);require(ready,'NOT_PREPARED','node has not prepared this plan')
        self.obs(c,n,ready['observation_id'])
        require(time.time()<ready['expires_at'],'PREPARE_EXPIRED','reservation expired')
    def public_plan(self,p):return clone(p)
    def tool(self,c,n,name,arg):
        node=c['nodes'][n]
        if name=='read_sensor_adapter':
            self.inject(c);node['sample_sequence']+=1
            ob={'observation_id':'obs_'+uuid.uuid4().hex[:12],'node_id':n,'group_task_id':c['id'],'activation':c['activations'][n],'sampled_at':time.time(),**clone(node),'measurements':self.measure(c)}
            c['observations'][ob['observation_id']]=ob;return ob
        if name=='read_peer_catalog':return {'peer_id':'B' if n=='A' else 'A','capabilities':['local_configuration','group_plan_response','prepare','apply'],'coordinator':'A'}
        if name=='send_peer_message':return self.peer(c,n,'collaboration_request' if arg.get('requires_response') else 'agent_message',arg['payload'])
        if name in ('read_peer_events','wait_peer_event'):
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
            rows=[clone(x) for x in c['messages'] if x['recipient']==n and x['sequence']>arg.get('after_sequence',0)]
            if rows:
                c['delivered'][n]=max(c['delivered'][n],max(x['sequence'] for x in rows))
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
            require(set(changes)=={'A','B'},'INVALID_PLAN','supply A and B plan fragments')
            for owner,v in changes.items():self.valid_actions(c,owner,v)
            require(changes['A']['path']==changes['B']['path'] and changes['A']['channel']==changes['B']['channel'],'INCOMPATIBLE_ENDPOINTS','both endpoints require same path and channel')
            pid='plan_'+uuid.uuid4().hex[:10]
            p={'plan_id':pid,'plan_revision':len(c['plans'])+1,'group_task_id':c['id'],'plan_hash':sha(changes),'node_actions':clone(changes),'proposer':n,'accepted':{n:True},'ready':{},'authorized':False,'applied':[]}
            c['plans'][pid]=p;self.peer(c,n,'plan_proposed',self.public_plan(p));return self.public_plan(p)
        if name=='read_group_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable');return self.public_plan(self.plan(c,arg['plan_id']))
        if name=='respond_group_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable');self.obs(c,n,arg['observation_id'])
            p=self.plan(c,arg['plan_id']);require(not p['authorized'],'PLAN_COMMITTED','cannot revise committed decision')
            require(type(arg['accept']) is bool,'INVALID_ARGUMENTS','accept must be boolean');p['accepted'][n]=arg['accept'];p['ready'].pop(n,None)
            self.peer(c,n,'plan_response',{'plan_id':p['plan_id'],'accept':arg['accept'],'explanation':arg.get('explanation','')});return self.public_plan(p)
        if name=='prepare_local_plan':
            require(not c['partition'],'PEER_UNAVAILABLE','node message channel unavailable')
            self.obs(c,n,arg['observation_id']);p=self.plan(c,arg['plan_id']);require(p['accepted'].get(n) is True,'NOT_ACCEPTED','accept plan first')
            v=p['node_actions'][n];port=node['ports'][v['path']]
            require(port['physical_up'],'PHYSICAL_UNAVAILABLE','selected local port is physically unavailable')
            require(.25+v['video_rate_mbps']<=max(0,port['capacity_mbps']-node['background_mbps']),'LOCAL_RESOURCE_LIMIT','requested local reservation exceeds available capacity')
            p['ready'][n]={'node_id':n,'observation_id':arg['observation_id'],'plan_hash':p['plan_hash'],'expires_at':time.time()+1800}
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
            require(min(x['sample_sequence'] for x in c['nodes'].values())>=4,'OBSERVATION_WINDOW','both participants must complete four active observations before ending')
            require(arg['status'] in ('solved','unresolved','needs_resources'),'INVALID_ARGUMENTS','unknown finish status')
            c['finished'][n]={'model_submission':clone(arg),'measurements':self.measure(c),'finished_at':time.time()}
            pending=[m for m in c['messages'] if m['recipient']==n and m['sequence']>c['delivered'][n] and m['kind']!='agent_message']
            if pending:self.activate_for_message(c,n,pending[0]['message_id'])
            return {'local_task_ended':True,'model_status':arg['status']}
        raise Fault('UNKNOWN_TOOL',name)
    def rpc(self,actor,data):
        with self.lock:
            op=data.get('op')
            if actor=='admin':
                if op=='begin_suite':
                    if self.state['active']:require(all(self.case(self.state['active'])['finished'].values()),'ACTIVE_CASE','previous suite still has live nodes')
                    self.state.update(suite_done=False,active=None,suite_name=data['name']);self.save();return {'started':data['name']}
                if op=='suite_end':self.state['suite_done']=True;self.save();return {'stopped':True}
                if op=='create':return {'case_id':self.create(data['fixture'])}
                if op=='snapshot':return clone(self.case(data['case_id']))
                if op=='force_end':
                    c=self.case(data['case_id'])
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
                if c['finished'][actor]:return {'job':None}
                return {'job':{'group_task_id':cid,'node_id':actor,'goal':GOAL,'cloud_available':False,'activation':c.get('activations',{}).get(actor,1),'remaining_decisions':64-c.get('decisions',{}).get(actor,0),'remaining_seconds':max(0,3600-(time.time()-c['created']))}}
            c=self.case(data['case_id'])
            if op=='register':
                boot=data['boot_id'];prior=c['boots'].get(actor)
                require(not prior or prior==boot,'EXPLICIT_RECOVERY_REQUIRED','previous node boot already registered; explicit recovery needed');c['boots'][actor]=boot;self.save();return {'registered':actor}
            require(c['boots'].get(actor)==data.get('boot_id'),'STALE_BOOT','node boot identity mismatch')
            if op=='trace':
                c['decisions'][actor]+=1
                self.event('model_turn',actor,{k:v for k,v in data.items() if k not in ('op','boot_id')});self.save();return {'recorded':True}
            if op=='runtime_end':c['finished'][actor]={'runtime_status':data['reason']};self.save();return {'ended':True}
            if op=='request_status':
                row=self.db.execute('SELECT result FROM requests WHERE id=?',(actor+':'+data['request_id'],)).fetchone();return {'status':'complete' if row else 'unknown','result':json.loads(row[0]) if row else None}
            require(op=='tool','UNKNOWN_OPERATION',str(op));require(not c['finished'][actor],'TASK_ENDED','local task already ended')
            rid=actor+':'+data['request_id'];fp=sha({k:data[k] for k in ('case_id','boot_id','name','arguments')})
            row=self.db.execute('SELECT fp,result FROM requests WHERE id=?',(rid,)).fetchone()
            if row:
                require(row[0]==fp,'REQUEST_ID_CONFLICT','same request_id with different operation');return json.loads(row[1])
            require('node_id' not in data['arguments'] or data['arguments']['node_id']==actor,'FORBIDDEN','cannot address another node executor')
            try:result={'ok':True,'result':self.tool(c,actor,data['name'],data['arguments'])}
            except Fault as e:result={'ok':False,'error_code':e.code,'error':str(e)}
            except (KeyError,TypeError,ValueError) as e:result={'ok':False,'error_code':'INVALID_ARGUMENTS','error':str(e)}
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
                'changed':c['changed'],'prepare_change_observed':c['stale_injected'],'peer_partition_observed':c['partition']}
