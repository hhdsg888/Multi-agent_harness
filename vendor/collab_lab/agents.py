import json
import time
from .common import dumps, write_json, read_json
from .candidates import DEVICE_API
from .service_interface import routes
from .tool_messages import cloud_delivery_message
from .runtime import RuntimeFault, RepetitionLimit, error_packet, explicit_versions, signature
from .resilience import checkpoint
from .capabilities import api as capability_api

EDGE_PROMPT = """你是端侧智能体，独立负责观察、分析、计划、工具组织、执行和效果验证。
你可以自主选择调查顺序、组合工具、生成约束内配置；已有能力可解决就本地处置。需要总体规划支持可request_cloud_plan，缺少必要工具能力才report_gap。
云端断联时报告会本地排队，你仍可用已有工具自主处置；不要重复提交相同报告。需要等待所缺能力时可wait_for_cloud，也可read_cloud_status查看。恢复只投递实际结果，不会替你采用新工具或执行历史方案。
能力报告采用模板辅助填充：先get_report_template读取草稿，再根据真实证据填充或改写，最后report_gap提交。模板不是固定内容schema，所有原字段均可增删、改名或重组；无需保留或填满全部字段，框架不替你写结论。
工具返回事实、计算或执行回执；不要把知识中的案例当当前事实，不要把推演预测当实际执行。
主动调用read_sensor_adapter读取现场，得到observation_id、sample_sequence、config_version及environment_revision。任务task_id/修订、采样身份、配置版本、工具目录catalog_revision及单工具版本彼此独立。采样不修改配置，发布工具也不修改配置。
优先使用业务接口：read_sensor_adapter观察，list_routes查询路径，再用simulate_services推演、apply_services执行业务配置。
services数组每项为flow_id、可选path_id、可选source_limit_mbps。path_id必须原样取自list_routes，不要填节点列表。
configure_device配置已有协议设备。无需拼装底层设备操作数组，使用业务接口即可控制路由和源速率。
发布的新工具可以从read_catalog查看，用input传工具参数、observation_id引用真实观测回执、preview选择推演或实际执行。
工具目录区分只读诊断和网络配置，并说明capabilities；只读诊断不能修改现场，通用配置不能越过设备协议。周期巡检任务也须主动读取传感器，可check_health登记检查；finish会基于你引用的真实观测完成同样的独立核验。程序登记的异常不等于你已完成根因诊断或业务修复。
工具可能来自持久仓库；目录标明已加载版本和兼容范围。工具职责内真实执行试用成功后可供后续会话复用，预览不算实际试用；这不代表业务已解决。当前会话固定版本，工具被禁用后不可继续调用。是否适用仍由你自主判断。
一个finish只结束当前任务，服务继续等下一任务。工具发布不等于本任务完成。结束前核验实际业务。无物理可用资源时可finish needs_resources，报告资源需求，不虚构成功。
工作方法：先比较实测和task，识别未满足的约束；再调查可用资源及可调整的路由、源速率、设备配置。
若推演不满足目标，应分析瓶颈并修订候选，不要把失败候选原样执行后就结束。一次方案失败不等于现有能力不足。
可通过retrieve_knowledge获取计算模型和处置原则；不要只重复当前配置。现有工具无法实现所需操作时报告缺口；需要更高层规划时请求云端策略。
finish unresolved需要说明尚未解决的具体瓶颈；有可用工具和资源时继续尝试合理候选。无需穷举全部组合。
只返回一个JSON对象：{"action":"实际工具名","arguments":{...},"reason":"简要依据"}。reason是可选说明，动作与参数必须完整。不输出思维过程或markdown。
所有路径/推演/配置及finish须传真实observation_id；操作后重新读传感器再核验。禁止手填expected_version或猜配置版本。可check_resource_feasibility核对必要物理路径，无路径时finish needs_resources。步数和速率用JSON数字，enabled/preview用JSON布尔值；不要给这些值加引号。
调用格式错误时先依据工具契约修正参数；格式错误本身不代表缺少专业工具能力。可以检索知识和查询工具清单辅助判断。
"""

CLOUD_TOOLS = {
    "read_context": "读取端侧报告原文、独立实测快照和当前任务",
    "read_catalog": "读取端侧真实工具清单",
    "list_routes": "查询真实拓扑的全部业务路径及可用状态，不修改现场",
    "read_device_api": "读取可制作工具的模拟设备API和函数契约",
    "write_candidate": "创建真实候选源码。arguments包括name、description、protocol、input_schema、code；只含build_operations函数",
    "test_candidate": "执行独立接口测试，arguments.candidate_id为真实身份",
    "read_candidate": "读取真实候选源码、描述及测试，arguments.candidate_id",
    "publish_candidate": "测试通过后发布新工具并通知端侧，arguments.candidate_id；发布由你自主决定",
    "submit_strategy": "提交高阶策略并交还端侧。arguments.strategy应说明目标、角色/资源安排、约束与验证建议；端侧自行落实",
    "finish_failure": "如实结束无法完成的云端任务，arguments.reason说明证据和所需资源",
}

CLOUD_PROMPT = """你是云端Qwen，支持端侧宏观规划和按需工具制作。先依据真实上下文调查，不虚构设备或工具。
mode=planning时输出高阶目标、资源/角色安排、约束和验证建议，由Gemma自主落地；你不直接修改现场。
你只能调用本提示词中的云端工具；read_catalog返回的是端侧工具目录，供规划参考，不能由你直接调用其中的执行工具。
mode=toolmaking时先检查现有能力和设备API，若确有缺口可制作、测试并自主发布新工具。
允许改进同名动态工具为新候选版本，但不能覆盖预置或框架工具。发布先供当前会话试用，真实执行验证工具职责后才进入跨会话复用；不要以接口测试通过冒充业务恢复。
候选是无外部访问的纯Python函数build_operations(arguments, device)。支持rescue-v2、network-control-v1（路由/源速率/standard-v1设备配置）、network-diagnostic-v1（只读业务目标诊断）。先read_device_api读取字段、声明能力和准确契约；不同类型返回不同结果。network-control-v1须声明capabilities，源代码由你实际编写，不绕过宿主权限。
不得仅口头声称写了工具；write_candidate、test_candidate和publish_candidate均需实际调用，测试失败可查证修订为新候选。
不需要训练模型或访问外部设备。物理资源缺失、现有工具已够用等情况应如实返回策略或失败依据。
现有工具的调用参数错误不等于新协议能力缺口。制作工具前核对真实设备协议和现有执行能力。
能力报告来自端侧对可编辑模板的填充或改写；字段名称和结构可以改变，不要求保留原模板字段。依据报告原文和独立状态调查。
每次只返回JSON：{"action":"工具名","arguments":{...},"reason":"简要依据"}，不要markdown或思维过程。
"""


def edge_loop(session, turn_budget=None):
    provider, evidence = session.providers, session.evidence
    if not session.edge_messages:
        session.edge_messages = [{"role":"user","content":dumps({"task":session.world.task,**session.tasks.identity()})}]
    guard = session.guard("edge")
    turns = 0
    try:
        while not session.closed.is_set() and not session.edge_finished:
            if turn_budget is not None and turns>=turn_budget:
                session.fail_task("failed_runtime",{"error_code":"VALIDATION_BUDGET","error":"外部验证调用额度用尽"})
                break
            if session.paused.is_set():
                session.edge_status = "paused"
                session.wake.wait(.3)
                session.wake.clear()
                continue
            if session.cloud_status in ("working","waiting_reconnect"):
                session.edge_status = "waiting_cloud"
                session.cloud_done.wait()
                continue
            if session.cloud_notification is not None:
                packet, session.cloud_notification = session.cloud_notification,None
                if packet.get("task_id")==session.tasks.active_id:
                    session.edge_messages.append({"role":"user","content":dumps(cloud_delivery_message(packet))})
            session.edge_status = "inference"
            appliance_prompt="""你是端侧智能体。当前sensor_contract=appliance-status-v1，主动read_sensor_adapter获取一体机10个只读状态接口。readings以原HTTP路径为键，body是原始接口JSON；外层编号为框架元数据。mock来源是模拟，不是现场实时读数。不能从rx_speed/rssi推算容量或voice/video达标率。卫星null表示未测得，不能当0。当前没有兼容的执行/推演工具，不能使用旧流体协议或虚构操作。可用call_appliance_api读取21条登记设备接口中的GET配置/状态；POST受宿主只读开关保护，不得靠换路径绕过。SOP mock不作为本模型决策事实。可检索参考、分析异常、取模板自主填报告或请求Qwen规划；报告字段自由。未提供业务测量时如实finish unresolved；模型提交solved也不能自动认证成功。每轮只返回JSON {action,arguments,reason}，结束/check_health引用真实observation_id。"""
            system = (appliance_prompt if getattr(session,'appliance',False) else EDGE_PROMPT)+"\n当前工具清单："+dumps(session.tools.catalog())
            messages = [{"role":"system","content":system},session.edge_messages[0]]+session.edge_messages[1:][-16:]
            identity = session.tasks.identity()
            answer = provider.call("edge",messages,session.tools.schemas(),evidence)
            turns += 1
            if session.closed.is_set() or identity!=session.tasks.identity():
                evidence.add("unexecuted_response","executor",{"call_id":answer["call_id"],"reason":"task_or_session_changed"})
                break
            while session.paused.is_set() and not session.closed.is_set():
                session.edge_status = "paused"
                session.wake.wait(.3)
                session.wake.clear()
            if session.closed.is_set():
                break
            value = answer["parsed"]
            session.edge_messages.append({"role":"assistant","content":answer["content"]})
            key = guard.before(value["action"],value["arguments"],session.guard_context())
            session.edge_status = "executing"
            try:
                if value["action"] not in session.tools.names():
                    raise RuntimeFault("TOOL_NOT_REGISTERED","工具未向端侧开放")
                result = session.tools.execute(value["action"],value["arguments"],request_id=identity["task_id"]+":"+answer["call_id"])
                guard.outcome(key,True)
                evidence.add("tool_result","edge",{"action":value["action"],"arguments":value["arguments"],"result":result,**identity})
            except Exception as error:
                result = error_packet(error)
                guard.outcome(key,False)
                evidence.add("tool_error","edge",{"action":value["action"],"arguments":value["arguments"],**result,**identity})
                if result["error_code"] in ("EXECUTION_OUTCOME_UNKNOWN","INTERNAL_ERROR"):
                    session.fail_task("failed_interface" if result["error_code"]=="EXECUTION_OUTCOME_UNKNOWN" else "failed_runtime",result)
                    break
                session.tasks.transition('investigating')
            session.edge_messages.append({"role":"user","content":dumps({"tool":value["action"],"result":result})})
            if not session.edge_finished and session.cloud_status not in ('working','waiting_reconnect'):
                from .assurance import record_progress
                record_progress(session)
            checkpoint(session)
    except RepetitionLimit as error:
        session.fail_task("failed_repetition",{**error_packet(error),"fingerprint":error.fingerprint,"count":error.count,"unexecuted_response":True})
    except Exception as error:
        details = error_packet(error)
        evidence.add("agent_error","edge",details)
        if details['error_code']=='TRANSPORT_UNAVAILABLE':
            session.waiting_edge_connection=True;session.edge_status='waiting_local_connection'
        else:
            session.fail_task("failed_interface" if details["error_code"].startswith(("MODEL_","TRANSPORT_")) else "failed_runtime",details)
    finally:
        session.save_result()


def cloud_loop(session,mode,report,observation,turn_budget=None,job=None,goal=None):
    evidence = session.evidence
    context = {"mode":mode,"task":goal,"report_original":report,"independent_observation":observation,**job}
    messages = [{"role":"system","content":CLOUD_PROMPT+"\n工具："+dumps(CLOUD_TOOLS)},
                {"role":"user","content":dumps({"mode":mode,**job,"instruction":"先调查真实上下文并自主完成本轮支持"})}]
    record=session.cloud_record(job)
    if record and record.get('messages'):messages=record['messages']
    guard = session.guard("cloud")
    turns, outcome = record.get('turns',0) if record else 0,None
    contract = signature(capability_api())
    try:
        while session.cloud_is_current(job):
            if turn_budget is not None and turns>=turn_budget:
                raise RuntimeFault("VALIDATION_BUDGET","外部云端验证调用额度用尽")
            pending=record.get('pending_action') if record else None
            if not pending and turns>=session.config.get('max_cloud_decisions',100):
                raise RuntimeFault('CLOUD_DECISION_LIMIT','云端作业达到预设决策上限，保留原报告与失败结果')
            answer = pending['answer'] if pending else session.providers.call("cloud",messages,CLOUD_TOOLS,evidence)
            turns += 0 if pending else 1
            if not session.cloud_is_current(job):
                evidence.add("unexecuted_response","executor",{"call_id":answer["call_id"],"reason":"stale_cloud_job"})
                break
            value, args = answer["parsed"],answer["parsed"]["arguments"]
            name = value["action"]
            if not pending:messages.append({"role":"assistant","content":answer["content"]})
            # Candidate identities are resolved to their content before repetition checks.
            normalized = dict(args)
            if "candidate_id" in args:
                candidate_path = session.candidates.path(args["candidate_id"])
                if (candidate_path/"tool.py").exists():
                    normalized["source_sha256"] = signature((candidate_path/"tool.py").read_text(encoding="utf-8"))
            key = pending['guard_key'] if pending else guard.before(name,normalized,{**job,"contract":contract})
            if not pending and name=="write_candidate" and isinstance(args.get("code"),str):
                guard.before_source(args["code"],args.get("protocol"),contract)
            if record:
                record.update(messages=messages,pending_action={'answer':answer,'guard_key':key},last_progress=time.time(),turns=turns)
                session.journal.job(record);checkpoint(session)
            try:
                if name=="read_context":
                    result = context
                elif name=="read_catalog":
                    result = session.tools.catalog()
                elif name=="list_routes":
                    if getattr(session,'appliance',False):raise RuntimeFault('SENSOR_CONTRACT_MISMATCH','接口提供系统路由事实，不提供旧业务path_id')
                    result = explicit_versions(session.business_tools.call(name,{},session.world) if session.business_tools else routes(session.world))
                elif name=="read_device_api":
                    result = {'sensor_contract':'appliance-status-v1','read_only':True,'writable_tool_families':[],
                        'sensor_endpoints':list(session.world.port.contract['endpoints']),
                        'note':'当前只完成真实传感器契约对齐，未声明真实设备执行或流体推演契约；不能制作旧协议工具假装可用。'} if getattr(session,'appliance',False) else capability_api()
                elif name=="write_candidate":
                    if getattr(session,"appliance",False):raise RuntimeFault("EXECUTION_CONTRACT_UNAVAILABLE","未登记一体机执行工具契约；不能发布旧流体工具")
                    if args.get("name") in session.tools.names() and args.get('name') not in session.candidates.published:
                        raise ValueError("名称已占用")
                    action_id=job['job_id']+':'+answer['call_id']
                    existing=next((read_json(p) for p in session.candidates.directory.glob('*/descriptor.json') if read_json(p).get('origin_action_id')==action_id),None)
                    result = existing or session.candidates.create({**args,'_runtime_action_id':action_id})
                elif name=="read_candidate":
                    ident = args["candidate_id"]
                    path = session.candidates.path(ident)
                    result = {"descriptor":session.candidates.descriptor(ident),"code":(path/"tool.py").read_text(encoding="utf-8")}
                    if (path/"test_result.json").exists():
                        result["test_result"] = read_json(path/"test_result.json")
                elif name=="test_candidate":
                    ident = args["candidate_id"]
                    result = session.candidates.test(ident)
                    code = (session.candidates.path(ident)/"tool.py").read_text(encoding="utf-8")
                    guard.tested_source(code,session.candidates.descriptor(ident)["protocol"],contract,result["passed"])
                elif name=="publish_candidate":
                    result = session.publish_candidate(args["candidate_id"],job)
                    outcome = {"type":"tool_published",**result}
                elif name=="submit_strategy":
                    result = {"strategy":args.get("strategy",args),"source":"qwen_original_response","execution_performed":False}
                    write_json(session.directory/"cloud"/job["job_id"]/"strategy.json",result)
                    outcome = {"type":"strategy",**result}
                elif name=="finish_failure":
                    result = {"reason":args.get("reason"),"source":"qwen_original_response"}
                    outcome = {"type":"cloud_unresolved",**result}
                else:
                    raise ValueError("不是可调用的云端工具")
                guard.outcome(key,not (name=="test_candidate" and not result["passed"]))
                evidence.add("tool_result","cloud",{"action":name,"arguments":args,"result":result,**job})
            except Exception as error:
                result = error_packet(error)
                guard.outcome(key,False)
                evidence.add("tool_error","cloud",{"action":name,"arguments":args,**result,**job})
            messages.append({"role":"user","content":dumps({"tool":name,"result":result})})
            limit=session.config.get('cloud_message_limit',32)
            if len(messages)>limit:messages=messages[:2]+messages[-limit+2:]
            if record:
                record.update(messages=messages,pending_action=None,last_progress=time.time())
                session.journal.job(record);checkpoint(session)
            write_json(session.directory/"cloud"/job["job_id"]/"conversation.json",messages)
            if outcome:
                break
    except RepetitionLimit as error:
        outcome = {"type":"cloud_error",**error_packet(error),"fingerprint":error.fingerprint,"count":error.count}
        session.fail_task("failed_repetition",{**outcome,"role":"cloud","unexecuted_response":True})
    except Exception as error:
        outcome = {"type":"cloud_error",**error_packet(error)}
        evidence.add("agent_error","cloud",outcome)
        if outcome['error_code']=='TRANSPORT_UNAVAILABLE':
            outcome['type']='cloud_deferred'
        elif outcome['error_code'].startswith(('MODEL_','TRANSPORT_')):
            session.fail_task('failed_interface',{**outcome,'role':'cloud'})
    finally:
        outcome = outcome or {"type":"cloud_interrupted","execution_performed":False}
        if outcome['type']=='cloud_deferred':session.defer_cloud(job,outcome)
        else:session.deliver_cloud(outcome,job)
