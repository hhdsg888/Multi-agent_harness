import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit, parse_qs
from . import BUILD
from .common import ROOT, dumps
from .environment import SCENARIOS, DEFAULT_TASK
from .providers import Providers
from .session import Session
from .tool_repository import ToolRepository
from .experience import ExperienceStore
from pathlib import Path
import re
from .resilience import RuntimeJournal


class Application:
    def __init__(self, config):
        self.config = config
        self.providers = Providers(config)
        self.session = None
        self.health_result = None
        self.lock = threading.RLock()
        self.repository = ToolRepository(config['tool_repository_directory'])
        self.experience=ExperienceStore(config.get('experience_directory') or Path(config['tool_repository_directory']).parent/'experience_store')

    def view(self):
        config = self.session.config if self.session else self.config
        return {"build": BUILD, "models": {r: config["models"][r]["model"] for r in ("edge", "cloud")},
                "model_health": self.health_result, "session": self.session.view() if self.session else None,
                "scenarios": {"normal":"一体机原接口模拟"} if config.get('sensor_profile','legacy-fluid')!='legacy-fluid' else SCENARIOS,
                "default_task": {'instruction':'主动读取一体机状态并调查异常，缺能力时上报，缺测量时如实说明。'} if config.get('sensor_profile','legacy-fluid')!='legacy-fluid' else DEFAULT_TASK, "sensor_profile":config.get("sensor_profile","legacy-fluid"),"environment_kind": "appliance_status" if config.get("sensor_profile", "legacy-fluid")!="legacy-fluid" else "fluid_network_simulation"}

    def start(self, arguments):
        with self.lock:
            if self.session and not self.session.closed.is_set():
                raise ValueError("请先结束当前会话，再新建场景")
            config, providers = dict(self.config), self.providers
            enabled=arguments.get('patrol_enabled',config.get('patrol_enabled',False))
            interval=arguments.get('patrol_interval_seconds',config.get('patrol_interval_seconds',60))
            if type(enabled) is not bool or type(interval) not in (int,float) or not 5<=interval<=86400:raise ValueError('巡检设置无效')
            config.update(patrol_enabled=enabled,patrol_interval_seconds=interval)
            self.session = Session(config, providers, arguments.get("scenario", "normal"), arguments.get("instruction"))
            self.session.start_simulation()
            if arguments.get("auto_run", False):
                self.session.start_edge()
            return self.session.view()

    def saved_sessions(self):
        root=Path(self.config.get('runs_directory') or ROOT/'runs');rows=[]
        for path in sorted(root.glob('*/runtime.sqlite3')):
            try:
                p=RuntimeJournal(path.parent).load()
                rows.append({'run_id':path.parent.name,'closed':p['closed'],'saved_at':p['saved_at'],'edge_status':p['edge_status']})
            except Exception as error:rows.append({'run_id':path.parent.name,'error':str(error)})
        return {'sessions':rows,'automatic_restore':False}

    def restore(self,arguments):
        with self.lock:
            if self.session and not self.session.closed.is_set():raise ValueError('先结束当前会话')
            ident=arguments.get('run_id','')
            if not re.fullmatch(r'\d{8}_\d{6}_[a-f0-9]{6}',str(ident)):raise ValueError('保存会话身份无效')
            root=Path(self.config.get('runs_directory') or ROOT/'runs').resolve();path=(root/ident).resolve();path.relative_to(root)
            if not (path/'runtime.sqlite3').exists():raise ValueError('保存会话不存在')
            self.session=Session(self.config,self.providers,restore_path=path)
            if arguments.get('auto_run',False):self.session.start_simulation();self.session.start_edge()
            return self.session.view()


def make_server(app, host=None, port=None):
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, pattern, *args):
            pass

        def send(self, value, status=200):
            data = dumps(value).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def allowed(self):
            # Local management service: reject DNS rebinding and cross-origin writes.
            host=self.headers.get('Host','').lower()
            expected={f'127.0.0.1:{self.server.server_port}',f'localhost:{self.server.server_port}'}
            if self.client_address[0] not in ('127.0.0.1','::1') or host not in expected:return False
            origin=self.headers.get('Origin')
            return origin is None or origin in ({'http://'+h for h in expected}|set(app.config.get('integration_allowed_origins',[])))

        def integration(self,path,method,body=None):
            from .appliance import enabled
            if not enabled(app.config):return False
            from .appliance_api import ApplianceAPI
            api=ApplianceAPI(app.config)
            if path=='/integration/interfaces' and method=='GET':self.send(api.catalog());return True
            if app.config.get('sensor_profile')!='appliance-live':return False
            try:api.route(path,method)
            except Exception as e:
                if getattr(e,'code',None)=='API_NOT_REGISTERED':return False
                self.send({'error_code':getattr(e,'code','INVALID_REQUEST'),'error':str(e)},405 if getattr(e,'code',None)=='API_METHOD_NOT_ALLOWED' else 400);return True
            try:
                r=api.request(path,method,body)
                self.send_response(r['http_status']);self.send_header('Content-Type',r['content_type'])
                self.send_header('Content-Length',str(len(r['bytes'])));self.send_header('Cache-Control','no-store')
                self.send_header('X-Integration-Source',r['data_source']);self.end_headers();self.wfile.write(r['bytes'])
            except Exception as e:self.send({'error_code':getattr(e,'code','UPSTREAM_ERROR'),'error':str(e)},403 if getattr(e,'code',None)=='APPLIANCE_READ_ONLY' else 502)
            return True

        def do_GET(self):
            if not self.allowed():return self.send({'error':'LOCAL_ORIGIN_REQUIRED'},403)
            path = urlsplit(self.path)
            try:
                if self.integration(self.path,"GET"):return
                if path.path.startswith('/command/'):
                    from .appliance import AppliancePort,enabled
                    if not enabled(app.config):return self.send({'error':'APPLIANCE_PROFILE_REQUIRED'},404)
                    response=AppliancePort(app.config).endpoint(path.path)
                    return self.send(response['body'],response['http_status'])
                if path.path == "/api/state":
                    return self.send(app.view())
                if path.path == "/api/events":
                    after = int(parse_qs(path.query).get("after", [0])[0])
                    return self.send({"events": app.session.evidence.recent(after) if app.session else []})
                if path.path == '/api/tasks':
                    return self.send(app.session.tasks.view() if app.session else {'active_task_id':None,'tasks':[]})
                if path.path == '/api/tool-repository':
                    return self.send(app.repository.view())
                if path.path == '/api/experiences':
                    return self.send(app.experience.view())
                if path.path == '/api/recovery':return self.send(app.saved_sessions())
                if path.path == '/api/runtime':return self.send(app.session.journal.status() if app.session else {'idle':True})
                if path.path == '/api/patrol':return self.send(app.session.patrol.view() if app.session else {'enabled':app.config.get('patrol_enabled',False),'service_started':False})
                if path.path == '/api/tasks/archive':
                    if not app.session:return self.send({'tasks':[]})
                    limit=max(1,min(200,int(parse_qs(path.query).get('limit',[100])[0])))
                    with app.session.journal.db() as db:return self.send({'tasks':[json.loads(r[0]) for r in db.execute('SELECT payload FROM archived_tasks ORDER BY rowid DESC LIMIT ?',(limit,))]})
                if path.path == '/api/cloud/outbox':
                    return self.send({'jobs':[{k:j.get(k) for k in ('identity','state','attempts','next_retry','last_progress','last_error')} for j in app.session.journal.jobs(limit=200)] if app.session else []})
                if path.path == "/api/protocol":
                    return self.send({"build": BUILD, "capabilities": (["local_autonomy","cloud_planning","capability_reporting","active_device_sensors","native_appliance_api","registered_interface_gateway","durable_outbox","explicit_process_recovery"] if app.config.get('sensor_profile','legacy-fluid')!='legacy-fluid' else ["local_autonomy", "cloud_planning", "tool_creation", "snapshot_simulation", "persistent_tool_repository", "verified_experience", "durable_outbox", "explicit_process_recovery"]),
                                      "sensor_profile":app.config.get('sensor_profile','legacy-fluid'),
                                      "sensor_contract":"appliance-status-v1" if app.config.get('sensor_profile','legacy-fluid')!='legacy-fluid' else "fluid-network-v1",
                                      "generated_tool_contracts":[] if app.config.get('sensor_profile','legacy-fluid')!='legacy-fluid' else ["rescue-v2","network-control-v1","network-diagnostic-v1"],
                                      "patrol":{"endpoint":"/api/patrol","explicit_agent_sensor_reads":True,"service_start_required":True},
                                      "tool_repository":{"endpoint":"/api/tool-repository","management_endpoint":"/api/tool-repository/action",
                                          "activation":"interface_tests_and_verified_live_tool_effect","existing_sessions":"version_pinned_with_disable_checks",
                                          "protocols":["rescue-v2","network-control-v1","network-diagnostic-v1"]},
                                      "report_content_schema": {},
                                      "capability_report_workflow":{"mode":"template_assisted","template_tool":"get_report_template","fields_editable":True,"requires_template_retrieval":True},
                                      "simulation_is_physical_device": False,
                                      "runtime_protocol":{"task_identity":["task_id","task_revision"],"observation_reference":"observation_id",
                                          "configuration":"config_version","environment":"environment_revision","catalog":"catalog_revision",
                                          "repetition_limit":app.config.get('repetition_limit',20),"persistent_agent_service":True}})
                filename = {"/": "index.html", "/app.js": "app.js", "/style.css": "style.css"}.get(path.path)
                if not filename:
                    return self.send({"error": "not_found"}, 404)
                data = (ROOT / "web" / filename).read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", {"html": "text/html; charset=utf-8", "js": "application/javascript; charset=utf-8", "css": "text/css; charset=utf-8"}[filename.rsplit(".", 1)[-1]])
                self.send_header("Cache-Control", "no-store")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as error:
                self.send({"error": f"{type(error).__name__}: {error}"}, 400)

        def do_POST(self):
            if not self.allowed():return self.send({'error':'LOCAL_ORIGIN_REQUIRED'},403)
            try:
                length = int(self.headers.get("Content-Length", 0))
                if not 0 <= length <= 1024 * 1024:
                    raise ValueError("请求过大")
                arguments = json.loads(self.rfile.read(length) or b"{}")
                if not isinstance(arguments, dict):
                    raise ValueError("请求须为JSON对象")
                path = urlsplit(self.path).path
                if self.integration(self.path,"POST",arguments):return
                if path == "/api/models/check":
                    app.health_result = (app.session.providers if app.session else app.providers).health()
                    return self.send(app.health_result)
                if path == "/api/session/start":
                    return self.send(app.start(arguments))
                if path == '/api/tool-repository/action':
                    return self.send(app.repository.manage(arguments.get('action'),arguments.get('name'),
                        arguments.get('version'),arguments.get('reason',''),arguments.get('expected_revision')))
                if path == '/api/experiences/action':
                    return self.send(app.experience.manage(arguments.get('experience_id'),arguments.get('enabled'),arguments.get('reason'),arguments.get('expected_sha256')))
                if path == '/api/session/migrate':
                    if app.session and not app.session.closed.is_set():raise ValueError('先结束当前服务会话')
                    from .migration import migrate
                    import hashlib
                    ident=arguments.get('run_id','')
                    if not re.fullmatch(r'\d{8}_\d{6}_[a-f0-9]{6}',str(ident)):raise ValueError('断点身份无效')
                    root=Path(app.config.get('runs_directory') or ROOT/'runs').resolve();directory=(root/ident).resolve();directory.relative_to(root)
                    if not (directory/'runtime.sqlite3').exists():raise ValueError('断点不存在')
                    hashes={str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'collab_lab').glob('*.py')}
                    return self.send(migrate(directory,hashes,arguments.get('reason')))
                if path == '/api/experiences/verify':
                    return self.send(app.experience.verify(arguments.get('experience_id')))
                if path == '/api/session/restore':return self.send(app.restore(arguments))
                session = app.session
                if not session or session.closed.is_set():
                    raise ValueError("请先创建会话")
                if path == "/api/edge/start":
                    session.start_simulation()
                    result = session.start_edge()
                elif path == '/api/patrol':
                    from .resilience import checkpoint
                    with session.lock:
                        result=session.patrol.configure(arguments.get('enabled'),arguments.get('interval_seconds'))
                        checkpoint(session)
                elif path == '/api/tasks':
                    result = session.enqueue_task(arguments.get('instruction'),arguments.get('goals'))
                    if arguments.get('auto_run',True):
                        session.start_simulation()
                        session.start_edge()
                elif path == '/api/tasks/revise':
                    result = session.revise_task(arguments.get('task_id'),arguments.get('instruction'))
                elif path == '/api/tasks/resume':
                    result = session.resume_resources(arguments.get('task_id'))
                    session.start_edge()
                elif path == "/api/edge/pause":
                    result = session.pause()
                elif path == "/api/session/end":
                    session.close()
                    result = {"ok": True}
                elif path == "/api/cloud/plan":
                    session.start_simulation()
                    result = session.start_cloud("planning", arguments.get("request", "请结合当前任务和状态提出总体策略，由端侧自行落实"))
                    session.start_edge()
                elif path == '/api/cloud/retry':
                    job=session.journal.lookup_job(arguments.get('job_id'))
                    if not job or job['state']!='retry_exhausted' or job['identity']['task_id']!=session.tasks.active_id:raise ValueError('仅可重试当前任务已耗尽的原云端请求')
                    job.update(state='retry',attempts=0,next_retry=0);session.journal.job(job)
                    from .resilience import start_supervisor
                    start_supervisor(session);result={'ok':True,'original_request_requeued':True,**job['identity']}
                elif path == "/api/environment/change":
                    result = session.world.perturb(arguments)
                    session.evidence.add("environment_changed", "user", {"changes": arguments, "version": result["version"], "model_notification_sent": False})
                else:
                    return self.send({"error": "not_found"}, 404)
                self.send(result)
            except (BrokenPipeError, ConnectionResetError):
                return
            except Exception as error:
                self.send({"error": f"{type(error).__name__}: {error}"}, 400)
    return ThreadingHTTPServer((host or app.config["host"], app.config["port"] if port is None else port), Handler)
