"""Schedule inspection tasks; assess only observations explicitly read by the agent."""
import copy,time
from .capabilities import diagnostic_truth
from .runtime import TERMINAL


class Patrol:
    def __init__(self,session):
        self.session=session
        self.data={'enabled':bool(session.config.get('patrol_enabled',False)),
            'interval_seconds':session.config.get('patrol_interval_seconds',60),
            'base_goal':copy.deepcopy(session.world.task),'last_scheduled':0,'last_checked':None,
            'last_cycle':None,'bad_streak':0,'good_streak':0,'incident_active':False,
            'occurrence':0,'last_emitted':None,'last_findings':[],'checks':0,'discoveries':0,'deduplicated':0,
            'consecutive_runtime_failures':0,'stopped_reason':None}

    def schedule(self):
        s=self.session;now=time.time();d=self.data
        if not d['enabled'] or s.paused.is_set() or now-d['last_scheduled']<d['interval_seconds']:return False
        if any(r['state'] not in TERMINAL for r in s.tasks.rows):return False
        goal=copy.deepcopy(d['base_goal'])
        goal.update(task_kind='patrol',instruction='周期巡检：主动读取现场传感器，核对当前业务目标；可调用check_health登记实测检查，再如实结束本次巡检。不要使用历史观测替代当前现场，不自动假定存在故障。')
        row=s.tasks.enqueue(goal);d['last_scheduled']=now
        s.evidence.add('patrol_scheduled','scheduler',{'task_id':row['task_id'],'environment_injected':False})
        s.wake.set();return True

    def assess(self,observation,source):
        if getattr(self.session,'appliance',False):
            from .appliance import health
            return health(self,observation,source)
        s=self.session;d=self.data;cycle=(observation['task_id'],observation.get('task_activation',1))
        truth=diagnostic_truth(observation)
        findings=[]
        if not truth['voice_target_met']:findings.append({'rule':'voice_target_unmet','measured':observation['metrics']['voice'].get('demand_satisfaction'),'required':observation['task']['voice_min_satisfaction']})
        if not truth['video_floor_met']:findings.append({'rule':'video_floor_unmet','measured':observation['metrics']['video'].get('delivered_mbps'),'required':observation['task']['video_min_mbps']})
        result={'observation_id':observation['observation_id'],'checks':truth,'findings':findings,
            'source':source,'observation_driven':True,'remediation_enqueued':False,'business_recovered':truth['objective_met']}
        if list(cycle)==d['last_cycle']:
            result['duplicate_inspection_cycle']=True;return result
        d.update(last_cycle=list(cycle),last_checked=time.time(),last_findings=findings,checks=d['checks']+1)
        if findings:
            d['bad_streak']+=1;d['good_streak']=0
            confirmed=d['bad_streak']>=s.config.get('patrol_confirmations',2)
            pending=any(r['goal'].get('task_kind')=='remediation' and r['state'] not in TERMINAL for r in s.tasks.rows)
            cooldown=not d['incident_active'] or d['last_emitted'] is None or time.time()-d['last_emitted']>=s.config.get('patrol_cooldown_seconds',300)
            room=sum(r['state']=='queued' for r in s.tasks.rows)<s.config.get('patrol_queue_limit',8)
            if confirmed and d['enabled'] and not pending and cooldown and room:
                if not d['incident_active']:d['occurrence']+=1
                d.update(incident_active=True,last_emitted=time.time(),discoveries=d['discoveries']+1)
                goal=copy.deepcopy(d['base_goal'])
                goal.update(task_kind='remediation',incident_id=s.ident+':'+str(d['occurrence']),
                    instruction='巡检已登记待处置事件。请主动重新读取现场，独立调查并保障当前业务目标；已有工具不能完成时再报告能力缺口。巡检旧观测只用于追溯，不能用来执行。')
                row=s.tasks.enqueue(goal);result.update(remediation_enqueued=True,remediation_task_id=row['task_id'])
                s.evidence.add('problem_discovered','patrol',{'task_id':observation['task_id'],'remediation_task_id':row['task_id'],
                    'observation_id':observation['observation_id'],'source':source,'findings':findings,'occurrence':d['occurrence'],'repair_selected':False})
                s.wake.set()
            elif confirmed:
                d['deduplicated']+=1;result['suppressed_by']='disabled_or_pending_or_cooldown_or_queue_limit'
            result['confirmed']=confirmed
        else:
            d['bad_streak']=0;d['good_streak']+=1
            if d['incident_active'] and d['good_streak']>=s.config.get('patrol_recovery_confirmations',2):
                d['incident_active']=False
                s.evidence.add('incident_cleared','patrol',{'observation_id':observation['observation_id'],'occurrence':d['occurrence']})
        s.evidence.add('patrol_assessed','patrol',result)
        return result

    def configure(self,enabled,interval=None):
        if type(enabled) is not bool:raise ValueError('enabled须为布尔值')
        if interval is not None:
            if type(interval) not in (int,float) or not 5<=interval<=86400:raise ValueError('巡检周期须为5至86400秒')
            self.data['interval_seconds']=interval
        self.data['enabled']=enabled
        if enabled:self.data.update(consecutive_runtime_failures=0,stopped_reason=None)
        if not enabled:
            for r in self.session.tasks.rows:
                if r['state']=='queued' and r['goal'].get('task_kind')=='patrol':r.update(state='cancelled',state_revision=r['state_revision']+1)
            self.session.tasks.save()
        self.session.wake.set()
        return self.view()

    def update_goal(self,goal):
        old=self.data['base_goal']
        if any(old.get(k)!=goal.get(k) for k in ('voice_min_satisfaction','video_min_mbps')):
            self.data.update(bad_streak=0,good_streak=0,last_cycle=None,incident_active=False,last_emitted=None)
            self.session.evidence.add('patrol_goal_changed','scheduler',{'streaks_reset':True})
        self.data['base_goal']=copy.deepcopy(goal)

    def runtime_failed(self,details):
        row=self.session.tasks.active();cycle=[row['task_id'],row.get('activation_sequence',1)] if row else None
        if cycle==self.data.get('last_failed_cycle'):return
        self.data['last_failed_cycle']=cycle
        self.data['consecutive_runtime_failures']+=1
        if self.data['consecutive_runtime_failures']>=self.session.config.get('patrol_failure_limit',3):
            self.data.update(enabled=False,stopped_reason='consecutive_inspection_runtime_failures')
            self.session.evidence.add('patrol_stopped','runtime',{'reason':self.data['stopped_reason'],'last_error':details})

    def view(self):
        return {**copy.deepcopy(self.data),'assessment_basis':'explicit agent observation and task service constraints',
            'scheduler_reads_sensors':False,'domain_rules':'current simulated voice/video goals; formal domain rules remain external'}
