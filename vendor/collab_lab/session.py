import copy
import hashlib
import threading
import uuid
from pathlib import Path
from datetime import datetime
from .common import ROOT, Evidence, write_json, now
from .environment import NetworkWorld
from .knowledge import KnowledgeBase
from .candidates import CandidateStore
from .tools import ToolRegistry, BUILTINS
from .tool_repository import ToolRepository, BACKEND
from .experience import ExperienceStore
from .patrol import Patrol
from .resilience import RuntimeJournal,RunLease,checkpoint,restore_fields,start_supervisor,storage_available,rotate_measurements
import time
from .agents import edge_loop, cloud_loop
from .toolchain import GeneratedToolchain
from .rag import RagLibrary
from .reporting import ReportTemplateTool
from .runtime import TaskLedger, RepetitionGuard, RuntimeFault, TERMINAL, explicit_versions, resource_feasibility


class Session:
    def __init__(self, config, providers, scenario='normal', instruction=None, run_root=None, initial_state=None, restore_path=None):
        self.config, self.providers = config, providers
        self.ident = datetime.now().strftime('%Y%m%d_%H%M%S_')+uuid.uuid4().hex[:6]
        self.directory = Path(restore_path) if restore_path else (run_root or Path(config.get('runs_directory') or ROOT/'runs'))/self.ident
        self.directory.mkdir(parents=True,exist_ok=True)
        self.journal=RuntimeJournal(self.directory)
        saved=self.journal.load() if restore_path else None
        if saved and saved['closed']:raise RuntimeFault('SESSION_CLOSED','已由用户结束的会话不能恢复')
        self.run_lease=RunLease(self.directory)
        if saved:
            from .appliance import enabled
            if enabled(config)!=(saved['world'].get('sensor_contract')=='appliance-status-v1'):
                self.run_lease.close();raise RuntimeFault('CHECKPOINT_SENSOR_CONTRACT','不能把旧流体断点当作一体机状态恢复')
            self.ident=self.directory.name;initial_state=saved['world']
        self.evidence = Evidence(self.directory,restore=bool(saved))
        self.lock = threading.RLock()
        from .appliance import enabled,ApplianceWorld
        self.appliance=enabled(config)
        self.world = ApplianceWorld(config,initial_state) if self.appliance else NetworkWorld(scenario,state=initial_state)
        if instruction:
            self.world.task['instruction'] = instruction
        self.tasks = TaskLedger(self.directory,self.evidence)
        if not saved:self.tasks.enqueue(self.world.task);self.tasks.activate_next()
        self.knowledge = KnowledgeBase()
        self.report_template = ReportTemplateTool(self.directory,config.get('report_template_file'))
        self.experience = ExperienceStore(config.get('experience_directory') or
            Path(config.get('tool_repository_directory') or ROOT/'tool_repository').parent/'experience_store')
        self._experience_captured=set()
        self.rag = RagLibrary(config['rag_pack'],None if saved else self.directory/'rag',config.get('strategy_pack'),self.experience) if config.get('rag_pack') else None
        self.business_tools = GeneratedToolchain(config,self.directory) if not self.appliance and config.get('tool_backend')=='qwen' else None
        self.candidates = CandidateStore(self.directory/'candidates',config['python'])
        self.repository = ToolRepository(config.get('tool_repository_directory') or
            ((run_root/'.tool_repository') if run_root else ROOT/'tool_repository'))
        self.catalog_revision = 1
        self.tools = ToolRegistry(self)
        artifacts, rejected = ([],[{'reason':'legacy fluid tool contracts not applicable to appliance-status-v1'}]) if self.appliance else self.repository.active(self.world.snapshot(), set(BUILTINS), config.get('execution_backend',BACKEND))
        for artifact in ([] if saved else artifacts):
            self.candidates.import_artifact(artifact)
        self.evidence.add('repository_loaded','runtime',{'loaded':[
            {'name':a['descriptor']['name'],'version':a['descriptor']['id'],'source_sha256':a['descriptor']['source_sha256']} for a in ([] if saved else artifacts)],
            'rejected':rejected,'repository_revision':self.repository.view()['revision']})
        self.closed, self.paused, self.wake, self.cloud_done = (threading.Event() for _ in range(4))
        self.cloud_done.set()
        self.edge_status, self.cloud_status = 'idle','not_requested'
        self.edge_finished, self.edge_result, self.cloud_result = False,None,None
        self.cloud_notification, self.cloud_job = None,None
        self.edge_messages = []
        self.edge_thread = self.cloud_thread = self.sim_thread = None
        self.error = None
        self.guards = {}
        self.supervisor_thread=None;self.waiting_edge_connection=False;self._restored_identity=None
        self.patrol=Patrol(self)
        self.runtime_hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'collab_lab').glob('*.py')}
        if saved:
            try:restore_fields(self,saved)
            except Exception:
                self.run_lease.close();raise
            return
        write_json(self.directory/'startup.json',{'id':self.ident,'scenario':scenario,'created':now(),
            'task':self.world.task,**self.tasks.identity(),'models':{r:config['models'][r]['model'] for r in ('edge','cloud')}})
        write_json(self.directory/'initial_state.json',self.world.snapshot())
        write_json(self.directory/'source_hashes.json',{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest()
            for folder,pattern in [('collab_lab','*.py'),('knowledge','*.json'),('strategies','*.json'),('templates','*.json')]
            for p in (ROOT/folder).glob(pattern)})
        write_json(self.directory/'inference_config.json',{k:v for k,v in config.items() if k.startswith(('edge_','cloud_','transport_','repetition_','observation_'))})
        self.evidence.add('session_created','user',{'id':self.ident,**self.tasks.identity(),'task':self.world.task})
        checkpoint(self)

    def guard(self,role):
        key = (self.tasks.active_id,role)
        if key not in self.guards:
            self.guards[key] = RepetitionGuard(self.config.get('repetition_limit',20))
        return self.guards[key]

    def guard_context(self):
        snapshot = self.world.snapshot()
        return {**self.tasks.identity(),'config_version':snapshot['version'],
                'environment_revision':snapshot.get('environment_revision',1)}

    def start_simulation(self):
        if self.sim_thread and self.sim_thread.is_alive():
            return
        def loop():
            while not self.closed.wait(self.config['simulation_period_seconds']):
                row = explicit_versions(self.world.tick())
                rotate_measurements(self)
                with (self.directory/'measurements.jsonl').open('a',encoding='utf-8') as stream:
                    import json
                    stream.write(json.dumps(row,ensure_ascii=False)+'\n')
                checkpoint(self)
        self.sim_thread = threading.Thread(target=loop,daemon=True,name='network-simulation')
        self.sim_thread.start()

    def start_edge(self,turn_budget=None):
        with self.lock:
            if self.closed.is_set():
                raise ValueError('会话已结束，请新建会话')
            self.paused.clear()
            start_supervisor(self)
            self.wake.set()
            if self.edge_thread and self.edge_thread.is_alive():
                return {'ok':True,'status':'resumed'}
            def dispatch():
                loaded = self._restored_identity
                while not self.closed.is_set():
                    with self.lock:
                        row = self.tasks.activate_next()
                        if row and (row['task_id'],row.get('activation_sequence'))!=loaded:
                            self.world.task = copy.deepcopy(row['goal'])
                            self.edge_finished, self.error = False,None
                            self.edge_result = None
                            self.edge_messages = []
                            self.tools.sensor.latest = None
                            loaded = (row['task_id'],row.get('activation_sequence'))
                            self.guards={}
                    if row is None:
                        self.edge_status = 'waiting_task'
                        with self.lock:
                            if self.patrol.schedule():checkpoint(self);continue
                        self.wake.wait(1 if self.patrol.data['enabled'] else None)
                        self.wake.clear()
                        continue
                    if self.waiting_edge_connection:
                        self.edge_status='waiting_local_connection';self.wake.wait(1);self.wake.clear();continue
                    if not storage_available(self):
                        self.edge_status='storage_blocked';self.wake.wait(2);self.wake.clear();continue
                    edge_loop(self,turn_budget)
                    self.save_result()
            self.edge_thread = threading.Thread(target=dispatch,daemon=True,name='edge-task-service')
            self.edge_thread.start()
            return {'ok':True,'status':'starting'}

    def enqueue_task(self,instruction=None,goals=None):
        with self.lock:
            if self.closed.is_set():
                raise RuntimeFault('SESSION_CLOSED','会话已结束')
            goal = copy.deepcopy(self.world.task)
            for key in ('task_kind','incident_id'):goal.pop(key,None)
            if instruction is not None:
                if not isinstance(instruction,str) or not instruction.strip():
                    raise ValueError('任务指令不能为空')
                goal['instruction'] = instruction
            if goals:
                for key,value in goals.items():
                    if key not in ('voice_min_satisfaction','video_min_mbps') or type(value) not in (int,float):
                        raise ValueError('未知任务目标或类型无效')
                    import math
                    if not math.isfinite(value) or value<0 or (key=='voice_min_satisfaction' and value>1):
                        raise ValueError('任务目标范围无效')
                    goal[key] = value
            row = self.tasks.enqueue(goal)
            self.patrol.update_goal(goal)
            self.wake.set()
            return row

    def resume_resources(self,task_id):
        with self.lock:
            row = next((r for r in self.tasks.rows if r['task_id']==task_id),None)
            if not row or row['state']!='blocked_resources':
                raise RuntimeFault('TASK_NOT_BLOCKED','目标任务不处于资源阻塞状态')
            if self.tasks.active() and self.tasks.active()['state'] not in TERMINAL:
                raise RuntimeFault('TASK_BUSY','请等待当前任务结束')
            snapshot = self.world.snapshot()
            revision = (snapshot['version'],snapshot.get('environment_revision',1))
            if tuple(row['result']['blocked_at_revision'])==revision:
                raise RuntimeFault('RESOURCES_UNCHANGED','现场未变化，不重复唤醒资源阻塞任务')
            row.update(state='queued',result=None)
            row.update(state_revision=row['state_revision']+1,updated_at=now())
            self.evidence.add('task_resources_requeued','user',{'task_id':task_id,'requires_new_observation':True})
            self.tasks.save()
            self.wake.set()
            return {'ok':True,'task_id':task_id,'requires_new_observation':True}

    def pause(self):
        self.paused.set()
        self.evidence.add('manual_pause','user',{'pending_model_response_is_not_executed_until_resumed':True})
        return {'ok':True}

    def revise_task(self,task_id,instruction):
        with self.lock:
            row = next((r for r in self.tasks.rows if r['task_id']==task_id),None)
            if row is None or row['state']!='queued':
                raise RuntimeFault('TASK_NOT_QUEUED','仅尚未执行的排队任务可直接修订；活动任务请另建任务')
            if not isinstance(instruction,str) or not instruction.strip():
                raise ValueError('任务指令不能为空')
            row['goal']['instruction'] = instruction
            row['task_revision'] += 1
            self.tasks.save()
            self.evidence.add('task_revised','user',{'task_id':task_id,'task_revision':row['task_revision']})
            return copy.deepcopy(row)

    def start_cloud(self,mode,report,turn_budget=None):
        with self.lock:
            row=self.tasks.active()
            if not row or row['state'] in TERMINAL:raise RuntimeFault('TASK_NOT_ACTIVE','没有活动任务')
            if mode not in ('planning','toolmaking'):raise ValueError('未知云端任务')
            from .runtime import signature
            duplicate=next((j for j in self.journal.jobs(pending=True) if j['identity']['task_id']==row['task_id'] and
                j['mode']==mode and signature(j['report'])==signature(report) and j['state'] in ('queued','retry','running')),None)
            if duplicate:return {'ok':True,**duplicate['identity'],'mode':mode,'status':duplicate['state'],'duplicate_report':True}
            if self.cloud_status=='working':raise RuntimeFault('CLOUD_BUSY','已有云端任务运行')
            if len(self.journal.jobs(pending=True))>=self.config.get('outbox_limit',16):
                raise RuntimeFault('OUTBOX_FULL','云端待发队列已到上限；原报告未丢失')
            report_id=uuid.uuid4().hex[:12]
            job={'job_id':'cloud_'+uuid.uuid4().hex[:12],'report_id':report_id,**self.tasks.identity()}
            observation=explicit_versions(self.world.observation())
            record={'identity':job,'mode':mode,'report':copy.deepcopy(report),'observation':observation,
                'goal':copy.deepcopy(row['goal']),'turn_budget':turn_budget,'state':'queued','attempts':0,
                'next_retry':0,'messages':None,'pending_action':None,'created_at':time.time()}
            self.journal.job(record)
            write_json(self.directory/'reports'/(report_id+'_original.json'),report)
            write_json(self.directory/'reports'/(report_id+'_evidence.json'),observation)
            method=self.report_template.provenance() if mode=='toolmaking' else {'mode':'planning_request'}
            write_json(self.directory/'reports'/(report_id+'_method.json'),{**method,**job})
            self.evidence.add('cloud_requested','edge',{'mode':mode,**job,'report_original':report,'report_method':method})
            self.launch_cloud_job(record)
            start_supervisor(self)
            return {'ok':True,**job,'mode':mode,'status':'waiting_for_real_cloud_result'}

    def launch_cloud_job(self,record):
        with self.lock:
            job=record['identity'];row=self.tasks.active()
            if self.closed.is_set() or not row or row['state'] in TERMINAL or self.tasks.identity()!={k:job[k] for k in ('task_id','task_revision')}:
                record['state']='stale';self.journal.job(record);return
            if self.cloud_status=='working':return
            if record['attempts']>=self.config.get('cloud_delivery_attempts',8):
                record['state']='retry_exhausted';self.journal.job(record);self.cloud_status='unresolved';self.cloud_done.set()
                self.cloud_notification={'type':'cloud_retry_exhausted','report_saved':True,**job};return
            record['state']='running';record['attempts']+=1;self.journal.job(record)
            self.cloud_job=job;self.cloud_status='working';self.cloud_done.clear();self.cloud_notification=None
            self.tasks.transition('waiting_cloud');checkpoint(self)
            self.cloud_thread=threading.Thread(target=cloud_loop,args=(self,record['mode'],record['report'],record['observation'],
                record['turn_budget'],copy.deepcopy(job),record['goal']),daemon=True,name='qwen-agent')
            self.cloud_thread.start()

    def cloud_record(self,job):
        return self.journal.lookup_job(job['job_id']) if job else None

    def defer_cloud(self,job,error):
        with self.lock:
            record=self.cloud_record(job)
            if record:
                record.update(state='retry',next_retry=time.time()+min(60,self.config.get('cloud_retry_seconds',10)*2**max(0,record['attempts']-1)),last_error=error)
                self.journal.job(record)
            if self.cloud_job==job:
                self.cloud_status='queued_offline';self.cloud_done.set()
                self.tasks.transition('investigating')
                self.cloud_notification={'type':'cloud_deferred','report_saved':True,'local_tools_available':True,**job}
            self.evidence.add('cloud_deferred','transport',{'report_saved':True,**job,'error':error})
            checkpoint(self)

    def cloud_is_current(self,job):
        return (not self.closed.is_set() and self.cloud_job==job and
                self.tasks.identity()=={k:job[k] for k in ('task_id','task_revision')} and
                self.tasks.active()['state'] not in TERMINAL)

    def publish_candidate(self,ident,job):
        with self.lock:
            if not self.cloud_is_current(job):
                raise RuntimeFault('STALE_CLOUD_RESULT','云端任务身份过期，未发布')
            self.tools.visible_candidates()
            candidate = self.candidates.descriptor(ident)
            if candidate['name'] in BUILTINS:
                raise RuntimeFault('RESERVED_TOOL_NAME','不能覆盖预置或框架工具')
            manifest = self.repository.stage(self.candidates, ident, {'session_id':self.ident,
                'author_model':self.config['models']['cloud']['model'],**job})
            descriptor = self.candidates.publish(ident,set(BUILTINS))
            descriptor.update(repository_version=ident, compatibility=manifest['compatibility'],
                              repository_state='staged_pending_live_trial')
            write_json(self.candidates.path(ident)/'published.json',descriptor)
            self.tools.visible_candidates()
            checkpoint(self)
            return {'ok':True,'published_tool':descriptor,'catalog':self.tools.catalog(),**job}

    def deliver_cloud(self,outcome,job):
        with self.lock:
            outcome = {**outcome,**job}
            if not self.cloud_is_current(job):
                self.evidence.add('cloud_result_quarantined','runtime',outcome)
                if self.cloud_job==job:
                    self.cloud_result = outcome
                    self.cloud_status = 'error' if outcome['type']=='cloud_error' else 'stale'
                    self.cloud_done.set()
                return False
            record=self.cloud_record(job)
            if record:
                record.update(state='complete',outcome=outcome,pending_action=None);self.journal.job(record)
            self.cloud_notification = outcome
            self.cloud_result = outcome
            self.cloud_status = 'error' if outcome['type']=='cloud_error' else ('unresolved' if outcome['type']=='cloud_unresolved' else 'complete')
            self.tasks.transition('investigating')
            self.cloud_done.set()
            self.evidence.add('cloud_delivery','transport',outcome)
            self.save_result()
            return True

    def fail_task(self,state,details):
        with self.lock:
            if self.tasks.active() and self.tasks.active()['goal'].get('task_kind')=='patrol':self.patrol.runtime_failed(details)
            self.edge_finished = True
            self.edge_status = state
            self.error = details.get('error')
            self.tasks.transition(state,details)
            if self.cloud_status=='working':
                if details.get('role')=='cloud':
                    self.cloud_result={**(self.cloud_job or {}),**details}
                self.cloud_status='error' if details.get('role')=='cloud' else 'cancelled'
                self.cloud_job=None
                self.cloud_done.set()
            self.evidence.add('runtime_protection','runtime',{'state':state,**self.tasks.identity(),**details})
            self.save_result()

    def finish_edge(self,arguments):
        with self.lock:
            if self.appliance:
                from .appliance import finish
                return finish(self,arguments)
            observation = self.tools.sensor.resolve(arguments.get('observation_id'))
            inspection=self.patrol.assess(observation,'runtime_finish_assessment') if self.tasks.active()['goal'].get('task_kind')=='patrol' else None
            status = arguments['status']
            feasibility = resource_feasibility(observation)
            resource_proven = not feasibility['necessary_resource_conditions_met']
            self.edge_result = {**self.tasks.identity(),'model_submission':copy.deepcopy(arguments),
                'independent_assessment':self.world.assess(),'completion_step':self.world.state['step'],
                'config_version':self.world.state['version'],'verified_solved':status=='solved' and self.world.assess()['objective_met']}
            if inspection:
                self.edge_result.update(inspection_completed=True,patrol_assessment=inspection)
                self.patrol.data['consecutive_runtime_failures']=0
            state = ('solved' if self.edge_result['verified_solved'] else 'unresolved') if status!='needs_resources' else ('blocked_resources' if resource_proven else 'unresolved')
            self.edge_result['resource_shortage_verified']=resource_proven
            if status=='unresolved' and not feasibility['necessary_resource_conditions_met']:
                state='blocked_resources'
                self.edge_result['resource_classification_source']='runtime_independent_physical_bound'
            if state=='blocked_resources':
                self.edge_result.setdefault('resource_classification_source','model_needs_resources_verified_by_runtime')
                self.edge_result.update(resource_evidence=feasibility,blocked_at_revision=[self.world.state['version'],self.world.state.get('environment_revision',1)])
            self.tasks.transition(state,self.edge_result)
            self.edge_finished, self.edge_status = True,'finished'
            self.evidence.add('task_finished','edge',self.edge_result)
            self.save_result()
            return self.edge_result

    def save_result(self):
        write_json(self.directory/'result.json',{'id':self.ident,'edge_status':self.edge_status,'cloud_status':self.cloud_status,
            'edge_result':self.edge_result,'cloud_result':self.cloud_result,'error':self.error,'tasks':self.tasks.view(),
            'last_assessment':self.world.assess(),'closed':self.closed.is_set(),'evidence_path':str(self.directory)})
        row=self.tasks.active()
        key=(row['task_id'],row.get('activation_sequence',1)) if row else None
        if row and row['state'] in TERMINAL and row.get('result') and key not in self._experience_captured:
            try:
                ident=self.experience.capture(self,row)
                verification=self.experience.verify(ident)
                self._experience_captured.add(key)
                self.evidence.add('experience_verified','runtime',{'experience_id':ident,**verification})
            except Exception as error:
                self.evidence.add('experience_error','runtime',{'task_id':row['task_id'],'error':str(error),'task_result_unchanged':True})
        checkpoint(self)

    def close(self):
        self.closed.set()
        for job in self.journal.jobs(pending=True):
            if job['state'] in ('queued','retry','running'):job['state']='cancelled';self.journal.job(job)
        self.wake.set()
        self.cloud_done.set()
        row = self.tasks.active()
        if row and row['state'] not in TERMINAL:
            self.tasks.transition('cancelled')
        self.evidence.add('session_closed','user',{'step':self.world.state['step']})
        self.save_result()
        if self.supervisor_thread and self.supervisor_thread is not threading.current_thread():self.supervisor_thread.join(10)
        self.run_lease.close()

    def view(self):
        return {'id':self.ident,'closed':self.closed.is_set(),'edge_status':self.edge_status,
                'service_active':bool(self.edge_thread and self.edge_thread.is_alive()),
                'edge_active':bool(self.edge_thread and self.edge_thread.is_alive() and self.tasks.active()),'paused':self.paused.is_set(),
                'cloud_status':self.cloud_status,'cloud_result':self.cloud_result,'edge_result':self.edge_result,
                'error':self.error,'tasks':self.tasks.view(),'world':explicit_versions(self.world.observation()),
                'history':self.world.snapshot()['history'],'assessment':self.world.assess(),
                'tools':self.tools.catalog(),'patrol':self.patrol.view(),'run_path':str(self.directory)}
