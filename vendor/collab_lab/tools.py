import copy
import uuid
from .common import write_json, read_json, digest
from .environment import ContractError
from .contracts import ARGUMENTS, PLUGIN
from .service_interface import routes, service_operations
from .sensor_adapter import SensorAdapter
from .feedback import effect_contract, task_progress
from .runtime import RuntimeFault, TERMINAL, explicit_versions, signature, resource_feasibility
from .tool_repository import BACKEND
from .capabilities import DIAGNOSTIC,plugin_schema,trial_verified


BUILTINS = {
    'call_appliance_api':('调用已登记的一体机原HTTP接口；GET读取配置/状态，POST必须引用观测且受宿主只读开关保护。SOP演示不作为本模型决策',{}),
    'check_health':('核验主动采样的业务目标并登记巡检事实；去抖确认后生成待处置任务，不选择修复方案',{}),
    'read_cloud_status':('查询已保存报告、云端作业与连接状态，不触发模型调用',{}),
    'wait_for_cloud':('自主选择等待已排队的云端真实结果；也可继续使用现有本地工具',{}),
    "read_sensor_adapter": ("主动调用独立传感器适配器读取节点、链路、业务、配置版本和实测指标；不返回路径方案或处置答案", {}),
    "list_routes": ("按真实拓扑列出每个业务的全部路径ID、可用状态、共享容量及未启用设备；不替你选择", {}),
    "simulate_services": ("推荐：按业务名、path_id及可选源速率推演效果，不改变现场；参数遵循input_schema", {}),
    "apply_services": ("推荐：按业务名、path_id及可选源速率实际应用配置；引用传感器返回的observation_id，运行层核对对应现场配置", {}),
    "configure_device": ("配置standard-v1设备管理开关和信道；不能修改物理up状态/容量。首版更换信道不模拟干扰消除，不能修复up=false；其他协议需要适配工具", {}),
    "inspect_links": ("读取链路状态、容量、设备协议与启用配置", {}),
    "inspect_services": ("读取真实模拟交付、需求与队列，不提供处置答案", {}),
    "read_catalog": ("读取当前工具清单和新发布工具的参数", {}),
    "retrieve_knowledge": ("查询演示知识/策略库", {"query": "字符串检索词"}),
    "simulate_plan": ("在冻结副本上计算计划后果；不会执行现场操作", {"operations": "操作数组", "steps": "1..30，可省略，默认5"}),
    "apply_plan": ("按expected_version原子执行1..12项操作并读取真实模拟效果", {
        "expected_version": "刚读取的整数配置版本", "operations": [
            {"op": "set_route", "flow_id": "voice或video", "route": ["链路id，按source到target顺序"]},
            {"op": "set_rate_limit", "flow_id": "voice或video", "limit_mbps": "源速率上限"},
            {"op": "configure_link", "link_id": "链路id", "enabled": "布尔值", "channel": "1..11整数；只支持standard-v1"}]}),
    "check_resource_feasibility": ("核对所引用观测中的必要物理路径；忽略管理开关，不能把协议不匹配当物理资源缺失", {}),
    "get_request_status": ("按真实request_id查询执行账本；unknown表示不得盲目重放写操作", {}),
    "get_receipt": ("读取之前真实工具执行回执", {"receipt_id": "实际回执身份"}),
    "get_report_template": ("获取可自由改写的能力报告草稿及填写说明。字段可增加、删除、改名或重组，不必保留原字段；填好后用report_gap提交", {}),
    "report_gap": ("先get_report_template再自主填充或改写草稿，提交报告请Qwen调查/制作工具；字段自由，不做报告内容schema校验，自动等待结果", {"report": "填充或改写后的任意JSON原文"}),
    "request_cloud_plan": ("需要总体规划支持时请求Qwen高阶策略，自动等待真实结果", {"request": "说明规划需求"}),
    "finish": ("完成本轮决策；系统另外核验实际业务，不能用自述代替实测", {"status": "solved / needs_resources / unresolved", "summary": "结果和依据"}),
}


class ToolRegistry:
    def __init__(self, session):
        self.session = session
        self.receipts = {}
        self.requests = {}
        self.sensor = SensorAdapter(session)
        self._visible_signature = None
        self.progress_facts = None

    def visible_candidates(self):
        with self.session.lock:
            visible = {name:descriptor for name,descriptor in self.session.candidates.published.items()
                       if not descriptor.get('repository_version') or self.session.repository.visible(name,descriptor['repository_version'])}
            current = tuple(sorted((name,d['id']) for name,d in visible.items()))
            if self._visible_signature is not None and current != self._visible_signature:
                self.session.catalog_revision += 1
            self._visible_signature = current
            return visible

    def names(self):
        # Keep the raw operation API for internal contract tests, not the model-facing API.
        if getattr(self.session,'appliance',False):
            return ['call_appliance_api','read_sensor_adapter','read_catalog','retrieve_knowledge','get_report_template','report_gap','request_cloud_plan','read_cloud_status','wait_for_cloud','get_request_status','get_receipt','check_health','finish']
        return [n for n in BUILTINS if n not in ("simulate_plan", "apply_plan", "call_appliance_api")] + list(self.visible_candidates())

    def schemas(self):
        return {t['name']:t['input_schema'] for t in self.catalog()['tools']}

    def mutating(self,name,arguments):
        if name=='call_appliance_api':return arguments.get('method','GET').upper()!='GET'
        return name in ('apply_services','configure_device','apply_plan') or (name in self.session.candidates.published and
            self.session.candidates.published[name]['protocol']!=DIAGNOSTIC and not arguments.get('preview',False))

    def catalog(self):
        tools = [{"name": name, "description": item[0], "input_schema": ARGUMENTS[name]} for name, item in BUILTINS.items() if name in self.names()]
        if self.session.business_tools:
            for tool in tools:
                if tool["name"] in ("observe","list_routes","inspect_links","inspect_services","retrieve_knowledge","simulate_services","apply_services","configure_device"):
                    tool["implementation"] = {"author_model":self.session.business_tools.manifest["author_model"],
                                              "source_sha256":self.session.business_tools.manifest["source_sha256"]}
        sensor_tool = next(t for t in tools if t["name"] == "read_sensor_adapter")
        if getattr(self.session,"appliance",False):
            sensor_tool["description"]="主动读取一体机10个只读状态接口；readings按原HTTP路径保留JSON层级、字段和类型，不包含旧仿真voice/video/容量。"
        sensor_tool["implementation"] = {"kind": "sensor_adapter", "source": self.sensor.port.source}
        if self.session.rag:
            tool = next(t for t in tools if t["name"] == "retrieve_knowledge")
            tool["description"] = "通过RAG检索知识库与历史策略库。query可用中文或英文症状、业务、设备、路由、容量和错误关键词；返回适用条件、处置经验和文档引用，不代表当前现场事实。"
            tool["implementation"] = {"kind":"rag","retriever":"framework_BM25",**self.session.rag.info()}
        for name, descriptor in self.visible_candidates().items():
            tools.append({"name": name, "description": descriptor["description"], "protocol": descriptor["protocol"],
                          "candidate_id": descriptor["id"], "source_sha256": descriptor["source_sha256"],
                          "input_schema": plugin_schema(descriptor["protocol"]), "capabilities":descriptor.get("capabilities",["device_command"]), "read_only":descriptor["protocol"]==DIAGNOSTIC, "tool_version": descriptor["id"], "tool_ref": descriptor["id"]+":"+descriptor["source_sha256"],
                          "repository_version":descriptor.get('repository_version'),
                          "loaded_from_repository":descriptor.get('loaded_from_repository',False),
                          "compatibility":descriptor.get('compatibility')})
        for tool in tools:
            tool["effects"] = effect_contract(tool["name"], tool.get("protocol"))
            tool.setdefault("tool_version", self.session.business_tools.manifest["id"] if tool.get("implementation",{}).get("author_model") else "runtime-v2")
        return {"catalog_revision": self.session.catalog_revision, "tools": tools}

    def execute(self, name, arguments, request_id=None):
        if not isinstance(arguments,dict):
            raise RuntimeFault("INVALID_ARGUMENTS","arguments须为对象")
        with self.session.lock, self.session.world.lock:
            if name=="get_request_status":
                record = self.requests.get(arguments.get("request_id")) or self.session.journal.lookup_request(arguments.get('request_id'))
                if record is None:
                    raise RuntimeFault("REQUEST_NOT_FOUND","没有该请求账本")
                return copy.deepcopy(record)
            request_id = request_id or "exec_"+uuid.uuid4().hex[:12]
            fingerprint = signature([name,arguments])
            existing = self.requests.get(request_id) or self.session.journal.lookup_request(request_id)
            if existing:
                if self.session.tasks.active_id and existing.get('task_id')!=self.session.tasks.active_id:
                    raise RuntimeFault('WRONG_TASK_REQUEST','旧任务请求不得作为当前任务执行')
                if existing["fingerprint"]!=fingerprint:
                    raise RuntimeFault("REQUEST_ID_REUSED","同一请求身份不得更换动作或参数")
                if existing["status"]=="succeeded":
                    return copy.deepcopy(existing["response"])
                raise RuntimeFault("EXECUTION_OUTCOME_UNKNOWN" if existing["status"] in ("running","unknown") else "REQUEST_ALREADY_FAILED","不重放已有请求；请查询回执并自主判断")
            row = self.session.tasks.active()
            if row is None or row["state"] in TERMINAL:
                raise RuntimeFault("TASK_NOT_ACTIVE","当前没有可执行任务")
            if row["state"]=="waiting_cloud":
                raise RuntimeFault("TASK_WAITING_CLOUD","当前任务等待实际云端交付")
            record = {"request_id":request_id,"fingerprint":fingerprint,"status":"running",
                      "action":name,"arguments":copy.deepcopy(arguments),**self.session.tasks.identity()}
            self.requests[request_id] = record
            self.session.journal.request(record)
            execution_version=self.session.world.state['version']
            write_json(self.session.directory/"requests"/(signature(request_id)+".json"),record)
            try:
                result = self._execute(name,copy.deepcopy(arguments))
                if name=="retrieve_knowledge":
                    result = copy.deepcopy(result)
                    if "version" in result:
                        result["corpus_version"] = result.pop("version")
                elif name not in ("get_report_template","report_gap","request_cloud_plan"):
                    result = explicit_versions(result)
                response = {**result,"request_id":request_id,**self.session.tasks.identity()}
                record.update(status="succeeded",response=copy.deepcopy(response))
                from .resilience import checkpoint
                checkpoint(self.session,record)
                while len(self.requests)>self.session.config.get('request_cache_limit',512):self.requests.pop(next(iter(self.requests)))
                while len(self.receipts)>self.session.config.get('receipt_cache_limit',512):self.receipts.pop(next(iter(self.receipts)))
                write_json(self.session.directory/"requests"/(signature(request_id)+".json"),record)
                return response
            except (TimeoutError,ConnectionError,OSError) as error:
                write = self.mutating(name,arguments)
                record.update(status="unknown" if write else "failed",error_type=type(error).__name__)
                self.session.journal.request(record)
                write_json(self.session.directory/"requests"/(signature(request_id)+".json"),record)
                if write:
                    raise RuntimeFault("EXECUTION_OUTCOME_UNKNOWN","写操作结果未知；request_id="+request_id+"；须查询该请求回执，不能重放") from error
                raise
            except Exception as error:
                write = self.mutating(name,arguments)
                uncertain=write and (self.session.world.state['version']!=execution_version or getattr(error,'code',None)=='EXECUTION_OUTCOME_UNKNOWN')
                record.update(status='unknown' if uncertain else 'failed',error_type=type(error).__name__)
                self.session.journal.request(record)
                write_json(self.session.directory/"requests"/(signature(request_id)+".json"),record)
                if uncertain:raise RuntimeFault('EXECUTION_OUTCOME_UNKNOWN','写操作已改变现场但完成记录失败；request_id='+request_id+'；禁止重放') from error
                raise

    def _execute(self, name, arguments):
        if getattr(self.session,"appliance",False) and name not in self.names():
            raise RuntimeFault("SENSOR_CONTRACT_MISMATCH","旧流体推演、配置和资源工具不适用于一体机只读状态契约")
        if name not in BUILTINS and name not in self.session.candidates.published:
            raise ContractError("工具未注册")
        if not isinstance(arguments, dict):
            raise ContractError("arguments须为对象")
        session, world = self.session, self.session.world
        generated = session.business_tools
        before = world.snapshot()
        bound = None
        if name in ("list_routes","simulate_services","apply_services","configure_device","simulate_plan","apply_plan","check_resource_feasibility","check_health") or name in session.candidates.published:
            if "expected_version" in arguments or "config_version" in arguments:
                raise RuntimeFault("VERSION_ARGUMENT_REMOVED","请引用observation_id；现场版本由观测回执关联，不接收手填版本")
            bound = self.sensor.resolve(arguments.get("observation_id"))
        if self.mutating(name,arguments):
            session.tasks.transition("acting")
        if name == 'call_appliance_api':
            from .appliance_api import ApplianceAPI
            path=arguments.get('path');method=arguments.get('method','GET').upper()
            api=ApplianceAPI(session.config);row,_=api.route(path,method)
            if row['upstream']!='device':raise RuntimeFault('MOCK_SOURCE_NOT_AGENT_TOOL','现有SOP演示接口只用于展示兼容，不能作为模型工具事实')
            if method!='GET':self.sensor.resolve(arguments.get('observation_id'))
            result=api.call(path,method,arguments.get('body'))
        elif name == 'check_health':
            result=session.patrol.assess(bound,'model_called_check_health')
        elif name == 'read_cloud_status':
            result={'cloud_status':session.cloud_status,'jobs':[{k:j.get(k) for k in ('identity','state','attempts','next_retry','last_progress')} for j in session.journal.jobs(limit=200)]}
        elif name == 'wait_for_cloud':
            pending=[j for j in session.journal.jobs() if j['identity']['task_id']==session.tasks.active_id and j['state'] in ('queued','retry','running')]
            if not pending:raise RuntimeFault('NO_PENDING_CLOUD','没有可等待的云端作业')
            session.cloud_status='waiting_reconnect';session.cloud_done.clear();session.tasks.transition('waiting_cloud')
            result={'waiting_for_actual_cloud_result':True,'reports_saved':True}
        elif name == "read_sensor_adapter":
            result = self.sensor.read(arguments)
        elif name == "list_routes":
            result = generated.call(name,arguments,world,bound) if generated else routes(world)
        elif name in ("simulate_services", "apply_services"):
            operations = generated.call("service_operations",arguments,world,bound) if generated else service_operations(world, arguments.get("services"))
            if name == "simulate_services":
                result = world.simulate(operations, arguments.get("steps", 5))
            else:
                result = world.apply(operations, bound["version"])
        elif name == "configure_device":
            operations = generated.call("device_operations",arguments,world,bound) if generated else [{"op": "configure_link", "link_id": arguments.get("link_id"),
                                  "enabled": arguments.get("enabled"), "channel": arguments.get("channel")}]
            result = world.apply(operations, bound["version"])
        elif name == "inspect_links":
            self.sensor.read({})
            result = generated.call(name,arguments,world,self.sensor.current()) if generated else {"version": before["version"], "step": before["step"], "nodes": before["nodes"], "links": before["links"]}
            result.update(observation_id=self.sensor.latest["observation_id"],sample_sequence=self.sensor.latest["sample_sequence"])
        elif name == "inspect_services":
            self.sensor.read({})
            result = generated.call(name,arguments,world,self.sensor.current()) if generated else {"version": before["version"], "step": before["step"], "flows": before["flows"], "assessment": world.assess()}
            result.update(observation_id=self.sensor.latest["observation_id"],sample_sequence=self.sensor.latest["sample_sequence"])
        elif name == "check_resource_feasibility":
            result = resource_feasibility(bound)
        elif name == "read_catalog":
            result = self.catalog()
        elif name == "retrieve_knowledge":
            if session.rag:
                result = session.rag.search(arguments.get("query", ""))
                session.evidence.add("rag_retrieval","edge",{"retrieval_id":result["retrieval_id"],"query":result["query"],
                    "corpus_sha256":result["corpus_sha256"],"hit_ids":[h["chunk_id"] for h in result["hits"]],
                    "collections":[h["collection"] for h in result["hits"]]})
            else:
                result = generated.call(name,arguments,world) if generated else session.knowledge.search(arguments.get("query", ""))
        elif name == "simulate_plan":
            result = world.simulate(arguments.get("operations"), arguments.get("steps", 5))
        elif name == "apply_plan":
            result = world.apply(arguments.get("operations"), bound["version"])
        elif name == "get_receipt":
            ident = arguments.get("receipt_id")
            if ident not in self.receipts:
                import re
                p=session.directory/'receipts'/(str(ident)+'.json')
                if re.fullmatch('[a-f0-9]{12}',str(ident)) and p.exists():self.receipts[ident]=read_json(p)
            if ident not in self.receipts:
                raise ContractError("不存在该回执")
            result = copy.deepcopy(self.receipts[ident])
        elif name == "get_report_template":
            result = session.report_template.get()
            session.evidence.add("report_template_retrieved","edge",session.report_template.provenance())
        elif name in ("report_gap", "request_cloud_plan"):
            mode = "toolmaking" if name == "report_gap" else "planning"
            if mode=="toolmaking":
                session.report_template.require_fetched()
            report = copy.deepcopy(arguments.get("report") if "report" in arguments else arguments)
            result = session.start_cloud(mode, report)
        elif name == "finish":
            if arguments.get("status") not in ("solved", "needs_resources", "unresolved"):
                raise ContractError("finish.status须为solved/needs_resources/unresolved")
            result = session.finish_edge(arguments)
        else:
            descriptor = session.candidates.published[name]
            inputs = arguments.get("input", {k: v for k, v in arguments.items() if k not in ("expected_version", "preview")})
            version = descriptor.get('repository_version')
            preview = arguments.get('preview',False)
            from contextlib import nullcontext
            lease = session.repository.execution_lease(name,version,bound,session.config.get('execution_backend',BACKEND)) if version else nullcontext()
            try:
                with lease as manifest:
                    operations = session.candidates.execute(descriptor["id"], inputs, bound,
                        expected_sha256=manifest['source_sha256'] if version else None)
                    if descriptor['protocol']==DIAGNOSTIC:
                        result={'ok':True,'diagnosis':operations,'scope':'read_only_diagnostic','read_only':True,'preview':preview}
                    elif preview:
                        result = world.simulate(operations, plugin_protocol=descriptor["protocol"])
                    else:
                        result = world.apply(operations, bound["version"], plugin_protocol=descriptor["protocol"])
            except ContractError as error:
                if version:
                    session.repository.record_trial(name,version,session.ident,False,preview,
                        {'error':str(error),'source_sha256':descriptor['source_sha256']})
                raise
            if version:
                after = world.snapshot()
                actual = after['links'].get(inputs.get('link_id'),{})
                verified = trial_verified(descriptor['protocol'],before,after,inputs,result,world.task) if not preview else False
                result['repository_trial'] = session.repository.record_trial(name,version,session.ident,preview or verified,preview,
                    {'source_sha256':descriptor['source_sha256'],'scope':result['scope'],
                     'before_sha256':digest(before),'after_sha256':digest(after),
                     'requested_input':inputs, 'actual_device_after':actual,
                     'source_session_path':str(session.directory),
                     'tool_effect_verified':verified if not preview else False,'business_objective_met':world.assess()['objective_met'],
                     'criterion':'declared tool effect independently verified; diagnostic is read-only; business recovery is separate'})
        if bound is not None:
            result.update(observation_id=bound["observation_id"])
        if result.get("scope")=="live_simulation" and not session.edge_finished:
            session.tasks.transition("verifying")
        result["task_progress"] = task_progress(session,name,result,before)
        ident = uuid.uuid4().hex[:12]
        receipt = {"receipt_id": ident, "tool": name, "arguments": copy.deepcopy(arguments), "result": result,
                   "before_sha256": digest(before), "after_sha256": digest(world.snapshot())}
        self.receipts[ident] = receipt
        write_json(session.directory / "receipts" / (ident + ".json"), receipt)
        if receipt["before_sha256"] != receipt["after_sha256"] and name not in ("read_sensor_adapter", "inspect_links", "inspect_services"):
            write_json(session.directory / "snapshots" / (ident + "_before.json"), before)
            write_json(session.directory / "snapshots" / (ident + "_after.json"), world.snapshot())
        return {"receipt_id": ident, **result}
