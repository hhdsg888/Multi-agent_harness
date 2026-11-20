"""Appliance GET contract mirror. No real-device write operations."""
import copy,json,threading,urllib.request,urllib.error
from concurrent.futures import ThreadPoolExecutor
from .common import ROOT,now,read_json
from .runtime import RuntimeFault,signature,TERMINAL

CONTRACT='appliance-status-v1'
def enabled(config):return config.get('sensor_profile','legacy-fluid') in ('appliance-mock','appliance-live')

class AppliancePort:
    def __init__(self,config):
        self.config=config;self.live=config.get('sensor_profile')=='appliance-live'
        self.source='appliance_http_read_only' if self.live else 'appliance_contract_simulation'
        self.contract=read_json(ROOT/'appliance_sensor_contract.json')
        self.fixture=read_json(ROOT/'appliance_sensor_mock.json')
        self.opener=urllib.request.build_opener(urllib.request.ProxyHandler({}))
    def endpoint(self,path):
        if path not in self.contract['endpoints']:raise RuntimeFault('SENSOR_ENDPOINT_NOT_ALLOWED','只允许登记的只读状态接口')
        if not self.live:return copy.deepcopy(self.fixture['readings'][path])
        from .appliance_api import ApplianceAPI
        response=ApplianceAPI(self.config).request(path)
        try:body=json.loads(response['bytes'])
        except Exception as e:raise RuntimeFault('SENSOR_INVALID_JSON','真实接口未返回JSON，不使用模拟回退') from e
        return {'http_status':response['http_status'],'body':body}
    def read(self):
        def one(path):
            try:
                r=self.endpoint(path);r['available']=r['http_status']==200 and isinstance(r['body'],dict) and r['body'].get('result')==0
                return path,r
            except Exception as e:return path,{'http_status':None,'available':False,'transport_error':type(e).__name__+': '+str(e)}
        with ThreadPoolExecutor(max_workers=4) as pool:return dict(pool.map(one,self.contract['endpoints']))

class ApplianceWorld:
    def __init__(self,config,state=None):
        self.lock=threading.RLock();self.port=AppliancePort(config);self.source=self.port.source
        self.task={'instruction':'主动读取一体机状态，调查异常并按已有能力处理；缺少能力时填写报告。未提供业务评分接口时如实说明无法独立判定业务达标。'}
        self.state=copy.deepcopy(state) if state else {'version':1,'environment_revision':1,'step':0,'history':[],
            'sensor_contract':CONTRACT,'readings':{}}
    def snapshot(self):
        with self.lock:return copy.deepcopy(self.state)
    def tick(self):
        with self.lock:self.state['step']+=1;return {'step':self.state['step'],'sensor_reads':0,'scope':'runtime_clock_only'}
    def observation(self):
        with self.lock:return {**self.snapshot(),'task':copy.deepcopy(self.task),'observed_at':now(),
            'sensor_source':self.port.source,'simulation':not self.port.live,'measurement_model':'endpoint JSON preserved; no voice/video/capacity inference'}
    def read(self):
        readings=self.port.read()
        with self.lock:
            if signature(readings)!=signature(self.state['readings']):self.state['environment_revision']+=1
            self.state['readings']=readings
            return self.observation()
    def assess(self):
        return {'objective_met':False,'objective_status':'not_observable','verified':False,'checks':{},'metrics':{},
            'scope':'device status endpoints do not measure per-business voice/video goals'}
    def perturb(self,args):raise RuntimeFault('SIMULATION_CONTROL_UNAVAILABLE','一体机状态镜像不支持旧流体容量/故障滑块')

def progress(session):
    sample=session.tools.sensor.latest
    return {'actual_state_known':bool(sample),'source':'explicit_sensor_sample' if sample else 'no_explicit_observation',
        'observation_id':sample.get('observation_id') if sample else None,'sensor_contract':CONTRACT,
        'objective_status':'not_observable','summary':'仅提供一体机接口事实；缺少语音/视频业务测量，未生成虚构达标率。'}

def finish(session,args):
    obs=session.tools.sensor.resolve(args.get('observation_id'))
    inspection=session.patrol.assess(obs,'runtime_finish_assessment') if session.tasks.active()['goal'].get('task_kind')=='patrol' else None
    result={**session.tasks.identity(),'model_submission':copy.deepcopy(args),'independent_assessment':session.world.assess(),
        'verified_solved':False,'resource_shortage_verified':False,'completion_step':session.world.state['step'],
        'unverified_reason':'business_measurement_and_resource_contract_unavailable'}
    if inspection:result.update(inspection_completed=True,patrol_assessment=inspection)
    session.edge_result=result;session.tasks.transition('unresolved',result)
    session.edge_finished=True;session.edge_status='finished';session.evidence.add('task_finished','edge',result);session.save_result()
    return result

def health(patrol,obs,source):
    # Detect failed reads/API errors only. Domain diagnosis requires explicit rules.
    d=patrol.data;s=patrol.session;cycle=[obs['task_id'],obs.get('task_activation',1)]
    findings=[{'rule':'sensor_endpoint_unavailable','endpoint':p,'http_status':r.get('http_status')} for p,r in obs['readings'].items() if not r.get('available')]
    result={'observation_id':obs['observation_id'],'source':source,'findings':findings,'observation_driven':True,
        'business_status':'not_observable','remediation_enqueued':False}
    if d['last_cycle']==cycle:return {**result,'duplicate_inspection_cycle':True}
    d.update(last_cycle=cycle,last_findings=findings,checks=d['checks']+1)
    d['bad_streak']=d['bad_streak']+1 if findings else 0;d['good_streak']=0 if findings else d['good_streak']+1
    import time
    pending=any(r['goal'].get('task_kind')=='remediation' and r['state'] not in TERMINAL for r in s.tasks.rows)
    room=sum(r['state']=='queued' for r in s.tasks.rows)<s.config.get('patrol_queue_limit',8)
    cooldown=d['last_emitted'] is None or time.time()-d['last_emitted']>=s.config.get('patrol_cooldown_seconds',300)
    if findings and d['bad_streak']>=s.config.get('patrol_confirmations',2) and d['enabled'] and not pending and room and cooldown:
        goal=copy.deepcopy(d['base_goal']);goal.update(task_kind='remediation',instruction='一体机只读状态接口异常，请重新主动读取并调查；不能把采集失败当作物理故障定论。')
        row=s.tasks.enqueue(goal);d.update(last_emitted=time.time(),discoveries=d['discoveries']+1,incident_active=True)
        result.update(remediation_enqueued=True,remediation_task_id=row['task_id']);s.evidence.add('problem_discovered','patrol',result);s.wake.set()
    if not findings and d['good_streak']>=s.config.get('patrol_recovery_confirmations',2):d.update(incident_active=False,last_emitted=None)
    s.evidence.add('patrol_assessed','patrol',result);return result
