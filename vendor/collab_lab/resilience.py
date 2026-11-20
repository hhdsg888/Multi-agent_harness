"""Durable checkpoints/outbox. Recovery restores facts, never chooses actions."""
import copy,json,sqlite3,time,hashlib,threading,os,shutil
from contextlib import contextmanager
from pathlib import Path
from collections import Counter
from .common import now
from .runtime import RuntimeFault,signature,TERMINAL


class RuntimeJournal:
    def __init__(self,directory):
        self.path=Path(directory)/'runtime.sqlite3'
        with self.db() as db:
            db.executescript('''CREATE TABLE IF NOT EXISTS checkpoint(id INTEGER PRIMARY KEY,payload TEXT,sha TEXT);
            CREATE TABLE IF NOT EXISTS requests(id TEXT PRIMARY KEY,payload TEXT);
            CREATE TABLE IF NOT EXISTS jobs(id TEXT PRIMARY KEY,payload TEXT);
            CREATE TABLE IF NOT EXISTS archived_tasks(id TEXT PRIMARY KEY,payload TEXT);''')
            if 'state' not in {r[1] for r in db.execute('PRAGMA table_info(jobs)')}:
                db.execute('ALTER TABLE jobs ADD COLUMN state TEXT')
                for r in db.execute('SELECT id,payload FROM jobs').fetchall():db.execute('UPDATE jobs SET state=? WHERE id=?',(json.loads(r['payload'])['state'],r['id']))
            db.execute('CREATE INDEX IF NOT EXISTS job_state_index ON jobs(state)')
    @contextmanager
    def db(self):
        db=sqlite3.connect(str(self.path),timeout=15);db.row_factory=sqlite3.Row
        try:yield db;db.commit()
        except Exception:db.rollback();raise
        finally:db.close()
    def save(self,payload,request=None):
        with self.db() as db:
            db.execute('INSERT OR REPLACE INTO checkpoint VALUES(1,?,?)',(json.dumps(payload,ensure_ascii=False),signature(payload)))
            if request:db.execute('INSERT OR REPLACE INTO requests VALUES(?,?)',(request['request_id'],json.dumps(request,ensure_ascii=False)))
    def request(self,record):
        with self.db() as db:db.execute('INSERT OR REPLACE INTO requests VALUES(?,?)',(record['request_id'],json.dumps(record,ensure_ascii=False)))
    def requests(self,limit=512):
        with self.db() as db:
            rows=db.execute('SELECT * FROM requests ORDER BY rowid DESC LIMIT ?',(limit,)).fetchall()
            return {r['id']:json.loads(r['payload']) for r in reversed(rows)}
    def lookup_request(self,ident):
        with self.db() as db:
            r=db.execute('SELECT payload FROM requests WHERE id=?',(ident,)).fetchone()
            return json.loads(r[0]) if r else None
    def uncertain_requests(self):
        with self.db() as db:return [p for r in db.execute('SELECT payload FROM requests') if (p:=json.loads(r[0]))['status'] in ('running','unknown')]
    def load(self):
        with self.db() as db:row=db.execute('SELECT * FROM checkpoint WHERE id=1').fetchone()
        if row is None:raise RuntimeFault('CHECKPOINT_MISSING','没有可恢复断点')
        payload=json.loads(row['payload'])
        if signature(payload)!=row['sha']:raise RuntimeFault('CHECKPOINT_INTEGRITY','断点完整性核验失败')
        return payload
    def job(self,job):
        with self.db() as db:db.execute('INSERT OR REPLACE INTO jobs(id,payload,state) VALUES(?,?,?)',(job['identity']['job_id'],json.dumps(job,ensure_ascii=False),job['state']))
    def jobs(self,pending=False,limit=None):
        query="SELECT payload FROM jobs"+(" WHERE state IN ('queued','retry','running')" if pending else '')+' ORDER BY rowid'
        if limit:query+=' DESC LIMIT '+str(int(limit))
        with self.db() as db:return [json.loads(r[0]) for r in db.execute(query)]
    def lookup_job(self,ident):
        with self.db() as db:
            row=db.execute('SELECT payload FROM jobs WHERE id=?',(ident,)).fetchone()
            return json.loads(row[0]) if row else None
    def status(self):
        with self.db() as db:
            return {'jobs':{r[0]:r[1] for r in db.execute('SELECT state,count(*) FROM jobs GROUP BY state')},
                'archived_tasks':db.execute('SELECT count(*) FROM archived_tasks').fetchone()[0],
                'durable_requests':db.execute('SELECT count(*) FROM requests').fetchone()[0],
                'checkpoint_saved_at':self.load()['saved_at']}


class RunLease:
    def __init__(self,directory):
        self.file=(Path(directory)/'runtime.lock').open('a+b');self.file.seek(0);self.file.write(b'0');self.file.flush();self.file.seek(0)
        try:
            if os.name=='nt':
                import msvcrt;msvcrt.locking(self.file.fileno(),msvcrt.LK_NBLCK,1)
            else:
                import fcntl;fcntl.flock(self.file,fcntl.LOCK_EX|fcntl.LOCK_NB)
        except OSError as error:
            self.file.close();raise RuntimeFault('SESSION_IN_USE','该会话仍由另一进程持有，不能并行恢复') from error
    def close(self):
        if not self.file.closed:self.file.close()


def checkpoint(s,request=None):
    with s.lock,s.world.lock:
        limit=s.config.get('runtime_message_limit',32)
        if len(s.edge_messages)>limit:s.edge_messages=[s.edge_messages[0]]+s.edge_messages[-limit+1:]
        history_limit=s.config.get('task_history_limit',128)
        completed=[r for r in s.tasks.rows if r['state'] in TERMINAL and r['task_id']!=s.tasks.active_id]
        if len(completed)>history_limit:
            expired=completed[:-history_limit]
            with s.journal.db() as db:
                for row in expired:db.execute('INSERT OR REPLACE INTO archived_tasks VALUES(?,?)',(row['task_id'],json.dumps(row,ensure_ascii=False)))
            ids={r['task_id'] for r in expired};s.tasks.rows=[r for r in s.tasks.rows if r['task_id'] not in ids];s.tasks.save()
        guards={}
        for (task,role),guard in s.guards.items():
            if task==s.tasks.active_id:guards[role]=copy.deepcopy(vars(guard))
        payload={'checkpoint_schema':2,'migration_history':getattr(s,'migration_history',[]),'world':s.world.snapshot(),'goal':s.world.task,'tasks':s.tasks.view(),
            'edge_messages':s.edge_messages,'edge_finished':s.edge_finished,'edge_result':s.edge_result,'error':s.error,
            'edge_status':s.edge_status,'cloud_status':s.cloud_status,'cloud_result':s.cloud_result,
            'cloud_notification':s.cloud_notification,'cloud_job':s.cloud_job,'published':s.candidates.published,
            'guards':guards,'closed':s.closed.is_set(),'sample_sequence':s.tools.sensor.sample_sequence,
            'provider_counter':getattr(s.providers,'counter',0),'experience_captured':[list(k) for k in s._experience_captured],
            'waiting_edge_connection':s.waiting_edge_connection,
            'patrol_state':s.patrol.data,
            'saved_at':now(),'runtime_hashes':s.runtime_hashes}
        s.journal.save(copy.deepcopy(payload),copy.deepcopy(request))


def restore_fields(s,p):
    from .common import read_json
    if p.get('checkpoint_schema',1) not in (1,2):raise RuntimeFault('CHECKPOINT_SCHEMA','不支持的断点版本')
    s.migration_history=p.get('migration_history',[])
    for name,sha in p['runtime_hashes'].items():
        from .common import ROOT
        if hashlib.sha256((ROOT/name).read_bytes()).hexdigest()!=sha:raise RuntimeFault('CHECKPOINT_RUNTIME_CHANGED','运行契约已改变，须显式迁移断点')
    if p['closed']:raise RuntimeFault('SESSION_CLOSED','已由用户结束的会话不能恢复')
    s.tasks.rows=p['tasks']['tasks'];s.tasks.active_id=p['tasks']['active_task_id'];s.world.task=p['goal']
    for name in ('edge_messages','edge_finished','edge_result','error','edge_status','cloud_status','cloud_result','cloud_notification','cloud_job'):
        setattr(s,name,p[name])
    s._experience_captured={tuple(k) for k in p.get('experience_captured',[])}
    s.candidates.published=p['published']
    for d in s.candidates.published.values():
        if hashlib.sha256((s.candidates.path(d['id'])/'tool.py').read_bytes()).hexdigest()!=d['source_sha256']:
            raise RuntimeFault('CHECKPOINT_TOOL_CHANGED','会话工具源码已变化')
    s.tools.requests=s.journal.requests()
    unknown=[]
    for record in s.journal.uncertain_requests():
        if record['status']=='running':record['status']='unknown';s.journal.request(record)
        if record['status']=='unknown' and record.get('task_id')==s.tasks.active_id and s.tools.mutating(record['action'],record.get('arguments',{})):unknown.append(record['request_id'])
        s.tools.requests[record['request_id']]=record
    s.tools.sensor.sample_sequence=p['sample_sequence'];s.tools.sensor.latest=None;s.tools.sensor.samples={}
    s.report_template.fetched=(s.directory/'report_template_returned.json').exists()
    s.waiting_edge_connection=p.get('waiting_edge_connection',False)
    if p.get('patrol_state'):s.patrol.data=p['patrol_state']
    if hasattr(s.providers,'counter'):
        ids=[int(f.name.split('_')[0]) for f in (s.directory/'calls').glob('*/*_request.json') if f.name.split('_')[0].isdigit()]
        s.providers.counter=max([s.providers.counter,p['provider_counter'],*ids])
    for role,attrs in p['guards'].items():
        guard=s.guard(role)
        for k,v in attrs.items():setattr(guard,k,Counter(v) if k in ('failures','source_failures','source_attempts','content_visits') else v)
    for job in s.journal.jobs(pending=True):
        if job['state']=='running':job['state']='queued';job['next_retry']=0;s.journal.job(job)
    s.cloud_status='queued_offline' if any(j['state'] in ('queued','retry') for j in s.journal.jobs(pending=True)) else s.cloud_status
    s.cloud_done.set();s._restored_identity=(s.tasks.active_id,s.tasks.active().get('activation_sequence')) if s.tasks.active() else None
    s.edge_messages.append({'role':'user','content':json.dumps({'event':'process_recovered','requires_new_explicit_observation':True,'unknown_write_requests':unknown})})
    if unknown:s.fail_task('failed_interface',{'error_code':'EXECUTION_OUTCOME_UNKNOWN','error':'恢复发现未确认写操作，禁止重放','request_ids':unknown})
    s.evidence.add('process_recovered','runtime',{'requires_new_observation':True,'unknown_write_requests':unknown})


def start_supervisor(s):
    if s.supervisor_thread and s.supervisor_thread.is_alive():return
    def loop():
        while not s.closed.wait(s.config.get('reconnect_poll_seconds',2)):
            try:
                if s.waiting_edge_connection:
                    if s.providers.health('edge')['edge']['ok']:
                        s.waiting_edge_connection=False;s.wake.set()
                for job in s.journal.jobs(pending=True):
                    if job['state'] not in ('queued','retry') or job.get('next_retry',0)>time.time():continue
                    with s.lock:
                        row=s.tasks.active()
                        if not row or row['state'] in TERMINAL or job['identity']['task_id']!=row['task_id'] or job['identity']['task_revision']!=row['task_revision']:
                            job['state']='stale';s.journal.job(job)
                            if s.cloud_job==job['identity']:s.cloud_status='stale';s.cloud_done.set()
                            continue
                        if s.cloud_status=='working':break
                    if not s.providers.health('cloud')['cloud']['ok']:
                        job['next_retry']=time.time()+s.config.get('cloud_retry_seconds',10);s.journal.job(job);continue
                    s.launch_cloud_job(job);break
                checkpoint(s)
            except Exception as error:
                s.evidence.add('supervisor_error','runtime',{'error':str(error)})
    s.supervisor_thread=threading.Thread(target=loop,daemon=True,name='connection-supervisor');s.supervisor_thread.start()


def storage_available(s):
    minimum=s.config.get('minimum_free_disk_mb',64)*1024*1024
    return shutil.disk_usage(s.directory).free>=minimum


def rotate_measurements(s):
    path=s.directory/'measurements.jsonl'
    if not path.exists() or path.stat().st_size<s.config.get('measurement_log_bytes',2*1024*1024):return
    count=s.config.get('measurement_log_archives',3)
    for n in range(count,0,-1):
        source=path if n==1 else s.directory/('measurements.'+str(n-1)+'.jsonl')
        target=s.directory/('measurements.'+str(n)+'.jsonl')
        if source.exists():os.replace(source,target)
