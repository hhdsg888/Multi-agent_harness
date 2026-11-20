"""Evidence-backed experience candidates; model claims never approve themselves."""
import copy
import json
import os
import sqlite3
import uuid
from pathlib import Path
from contextlib import contextmanager
from .common import digest, read_json, now
from .environment import NetworkWorld
from .runtime import RuntimeFault, signature


def configuration(state):
    return {'links':{k:{f:v[f] for f in ('source','target','protocol','up','enabled','channel','capacity_mbps')} for k,v in state['links'].items()},
        'flows':{k:{f:v[f] for f in ('source','target','route','requested_mbps','limit_mbps')} for k,v in state['flows'].items()},
        'nodes':state['nodes']}


class ExperienceStore:
    def __init__(self, directory):
        self.directory=Path(directory).resolve();self.directory.mkdir(parents=True,exist_ok=True)
        self.database=self.directory/'experience.sqlite3'
        with self.db() as db:
            db.execute('CREATE TABLE IF NOT EXISTS management_audit (id INTEGER PRIMARY KEY, payload TEXT NOT NULL)')
            db.execute('CREATE TABLE IF NOT EXISTS experiences (id TEXT PRIMARY KEY, payload TEXT NOT NULL, sha TEXT NOT NULL, status TEXT NOT NULL, verification TEXT, enabled INTEGER NOT NULL DEFAULT 1, created TEXT NOT NULL)')

    @contextmanager
    def db(self):
        db=sqlite3.connect(str(self.database),timeout=15);db.row_factory=sqlite3.Row
        try:
            yield db;db.commit()
        except Exception:
            db.rollback();raise
        finally:db.close()

    def capture(self, session, task):
        ident='exp_'+signature([session.ident,task['task_id'],task['task_revision'],task.get('activation_sequence',1)])[:24]
        samples=[read_json(p) for p in (session.directory/'sensor_reads').glob('*.json')]
        samples=sorted([s for s in samples if s.get('task_id')==task['task_id'] and s.get('task_revision')==task['task_revision'] and
            s.get('task_activation',1)==task.get('activation_sequence',1)],key=lambda s:s['sample_sequence'])
        ids={s['observation_id'] for s in samples}
        receipts=[]
        for receipt in (read_json(p) for p in (session.directory/'receipts').glob('*.json')):
            if receipt['arguments'].get('observation_id') not in ids or receipt['result'].get('scope')!='live_simulation':continue
            row=copy.deepcopy(receipt);rid=row['receipt_id']
            for suffix in ('before','after'):
                p=session.directory/'snapshots'/(rid+'_'+suffix+'.json')
                row[suffix]=read_json(p) if p.exists() else None
            receipts.append(row)
        receipts.sort(key=lambda r:(r['before']['version'] if r['before'] else -1,r['receipt_id']))
        events=[copy.deepcopy(e) for e in session.evidence.events if e['data'].get('task_id')==task['task_id'] and
            e['kind'] in ('tool_error','rag_retrieval','cloud_requested','cloud_delivery','tool_result')]
        payload={'session_id':session.ident,'source_directory':str(session.directory),'task':copy.deepcopy(task),
            'problem':copy.deepcopy(task['goal']),'observations':samples,'strategy_and_tool_events':events,
            'executions':receipts,'final_state':session.world.snapshot(),
            'tools':session.tools.catalog(),'final_assessment':session.world.assess(),
            'model_submission':copy.deepcopy((task.get('result') or {}).get('model_submission')),
            'scope':'fluid-network simulation; context-bound measured experience, not a general strategy guarantee'}
        with self.db() as db:
            db.execute('INSERT OR IGNORE INTO experiences(id,payload,sha,status,created) VALUES (?,?,?,?,?)',
                (ident,json.dumps(payload,ensure_ascii=False),signature(payload),'candidate',now()))
        return ident

    def verify(self, ident):
        with self.db() as db:
            row=db.execute('SELECT * FROM experiences WHERE id=?',(ident,)).fetchone()
            if not row:raise RuntimeFault('EXPERIENCE_NOT_FOUND','经验候选不存在')
            payload=json.loads(row['payload']);errors=[]
            if payload['final_state'].get('sensor_contract')=='appliance-status-v1':
                verification={'passed':False,'errors':['appliance_business_and_replay_contract_unavailable'],'strategy_publishable':False,'outcome':'unverified_appliance_observation'}
                db.execute('UPDATE experiences SET status=?,verification=? WHERE id=?',('rejected',json.dumps(verification),ident))
                return verification
            if signature(payload)!=row['sha']:errors.append('candidate_integrity_mismatch')
            samples=payload['observations'];task=payload['task'];goal=payload['problem'];final=payload['final_state']
            if goal!=task['goal']:errors.append('task_goal_mismatch')
            if not samples:errors.append('no_explicit_observation')
            sample_by_id={s['observation_id']:s for s in samples}
            for s in samples:
                if s.get('task_id')!=task['task_id'] or s.get('task_revision')!=task['task_revision']:errors.append('cross_task_observation')
            submission=payload.get('model_submission') or {}
            last=sample_by_id.get(submission.get('observation_id'))
            if not last:errors.append('no_final_explicit_verification')
            elif last['version']!=final['version'] or last.get('environment_revision',1)!=final.get('environment_revision',1):errors.append('stale_final_verification')
            elif configuration(last)!=configuration(final):errors.append('final_observation_configuration_mismatch')
            for r in payload['executions']:
                before,after=r['before'],r['after'];sample=sample_by_id.get(r['arguments'].get('observation_id'))
                if before is None or after is None:
                    errors.append('missing_execution_snapshots');continue
                if digest(before)!=r['before_sha256'] or digest(after)!=r['after_sha256']:errors.append('execution_snapshot_integrity')
                if not sample or sample['version']!=before['version'] or sample.get('environment_revision',1)!=before.get('environment_revision',1):errors.append('execution_observation_mismatch')
                elif configuration(sample)!=configuration(before):errors.append('execution_observation_configuration_mismatch')
                try:
                    twin=NetworkWorld(task=goal,state=before)
                    ops=r['result']['operations']
                    protocol=next((op['protocol'] for op in ops if op.get('op')=='device_command'),None)
                    twin.apply(ops,before['version'],plugin_protocol=protocol)
                    if twin.snapshot()!=after:errors.append('execution_replay_mismatch')
                except Exception as error:errors.append('execution_replay_failed:'+type(error).__name__)
            actual=NetworkWorld(task=goal,state=final).assess()
            if actual!=payload['final_assessment']:errors.append('independent_assessment_mismatch')
            # Recompute measured rates on a fresh queue step, not from a model claim.
            twin=NetworkWorld(task=goal,state=final);twin.tick()
            for fid,m in actual['metrics'].items():
                if abs(m['delivered_mbps']-twin.assess()['metrics'][fid]['delivered_mbps'])>1e-8:errors.append('measurement_recompute_mismatch')
            outcome='verified_solution' if actual['objective_met'] and payload['executions'] else 'verified_observation' if actual['objective_met'] else 'verified_failure'
            verification={'passed':not errors,'errors':sorted(set(errors)),'outcome':outcome,
                'independent_objective_met':actual['objective_met'],'execution_count':len(payload['executions']),
                'strategy_publishable':not errors and outcome=='verified_solution','verified_at':now(),
                'scope':'replayed actual operations and checked fresh measured evidence; no model-text approval'}
            db.execute('UPDATE experiences SET status=?,verification=? WHERE id=?',
                ('verified' if not errors else 'rejected',json.dumps(verification,ensure_ascii=False),ident))
        self.export()
        return verification

    def manage(self,ident,enabled,reason,expected_sha):
        if type(enabled) is not bool or not isinstance(reason,str) or not reason.strip():raise ValueError('需要布尔开关和管理原因')
        with self.db() as db:
            db.execute('BEGIN IMMEDIATE')
            row=db.execute('SELECT * FROM experiences WHERE id=?',(ident,)).fetchone()
            if not row or row['sha']!=expected_sha:raise RuntimeFault('EXPERIENCE_CONFLICT','候选不存在或原始哈希不匹配')
            if enabled and (row['status']!='verified' or signature(json.loads(row['payload']))!=row['sha']):
                raise RuntimeFault('EXPERIENCE_UNVERIFIED','未核验或损坏经验不能启用')
            db.execute('UPDATE experiences SET enabled=? WHERE id=?',(int(enabled),ident))
            event={'experience_id':ident,'enabled':enabled,'reason':reason.strip(),'at':now(),'sha':row['sha']}
            db.execute('INSERT INTO management_audit(payload) VALUES(?)',(json.dumps(event,ensure_ascii=False),))
        self.export()
        return event

    def documents(self):
        documents=[]
        with self.db() as db:
            rows=db.execute("SELECT * FROM experiences WHERE status='verified' AND enabled=1 ORDER BY id").fetchall()
            for row in rows:
                p=json.loads(row['payload']);v=json.loads(row['verification'])
                if signature(p)!=row['sha'] or not v.get('passed'):continue
                context={'links':{k:{f:l[f] for f in ('source','target','protocol','up','enabled','capacity_mbps')} for k,l in p['observations'][0]['links'].items()},
                    'flows':{k:{f:l[f] for f in ('route','requested_mbps','limit_mbps')} for k,l in p['observations'][0]['flows'].items()},'goal':p['problem']}
                measured={'objective_met':p['final_assessment']['objective_met'],'metrics':p['final_assessment']['metrics'],
                    'routes':{k:f['route'] for k,f in p['final_state']['flows'].items()}}
                common={'experience_id':row['id'],'source_session':p['session_id'],'source_task':p['task']['task_id'],
                    'evidence_sha256':row['sha'],'verification':v,'applicability':context,
                    'limitations':'仅为此观测/配置/工具范围的实测经验；必须重新主动采样、检查当前兼容性，不自动执行历史方案。'}
                documents.append({**common,'id':row['id']+'_facts','collection':'knowledge',
                    'title':'核验经验 '+v['outcome'],'tags':['实测','历史经验',*p['final_state']['links'].keys()],
                    'content':{'problem':p['problem'],'actual_result':measured,'outcome':v['outcome']}})
                if v['strategy_publishable']:
                    documents.append({**common,'id':row['id']+'_strategy','collection':'strategy',
                        'title':'已核验的具体处置轨迹','tags':['路由','链路','配置','处置经验'],
                        'content':{'actual_operations':[r['result']['operations'] for r in p['executions']],
                            'tool_sequence':[{'tool':r['tool'],'arguments':r['arguments'],
                            'tool_version':r['result'].get('repository_trial',{}).get('tool_version')} for r in p['executions']],
                            'actual_result':measured,'instruction':'历史观测身份不可重用；由端侧判断当前适用性并重新推演。'}})
        return documents

    def export(self):
        docs=self.documents()
        for collection,folder in (('knowledge','knowledge'),('strategy','strategies')):
            p=self.directory/folder/'verified.json';p.parent.mkdir(parents=True,exist_ok=True)
            temp=p.with_suffix('.tmp.'+uuid.uuid4().hex);temp.write_text(json.dumps({'collection':collection,'documents':[d for d in docs if d['collection']==collection]},ensure_ascii=False,indent=2),encoding='utf-8')
            os.replace(temp,p)

    def view(self):
        with self.db() as db:
            return {'candidates':[{'id':r['id'],'status':r['status'],'enabled':bool(r['enabled']),
                'source_sha256':r['sha'],'verification':json.loads(r['verification']) if r['verification'] else None,
                'source_session':json.loads(r['payload'])['session_id']} for r in db.execute('SELECT * FROM experiences ORDER BY created')],
                'audit':[json.loads(r[0]) for r in db.execute('SELECT payload FROM management_audit ORDER BY id DESC LIMIT 100')],
                'documents':len(self.documents()),'scope':'verified context-bound knowledge and strategies'}
