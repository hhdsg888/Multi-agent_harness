"""Qwen-authored business tools, independent verification, publication and pinning."""
import copy
import hashlib
import json
import re
import subprocess
import uuid
from pathlib import Path
from .common import ROOT, Evidence, read_json, write_json, now, dumps
from .environment import NetworkWorld, ContractError
from .knowledge import KnowledgeBase
from .service_interface import routes as reference_routes


FUNCTIONS = ("observe", "inspect_links", "inspect_services", "list_routes", "retrieve_knowledge",
             "service_operations", "device_operations")

REQUIREMENTS = """请编写业务工具Python模块，只含函数定义，不含import、注解、默认参数、装饰器、顶层执行。
工具不包含LLM调用、网络、文件或现场答案；所有状态来自context，不得硬编码节点、链路、flow身份和预期动作。
每个公开函数签名为(function arguments, context)，即def observe(arguments, context): ...，其他函数同样两个参数。
context.observation为真实观测，含version、step、nodes、links字典、flows字典、metrics、task、observed_at、measurement_model；
注意准确数据类型：nodes是数组，每个对象为{'id':字符串,'online':布尔值,'battery_soc':数字,...}，不是按ID索引的字典。
links是按ID索引的字典，每条{'source':节点ID,'target':节点ID,'capacity_mbps':数字,'up':布尔值,'enabled':布尔值,'protocol':字符串,'channel':整数}。
不存在node.status或link.status；必须读取node.online和link.up。flows按ID索引，每条包含source、target、route（链路ID数组）、requested_mbps、limit_mbps、queue_mbit。
context.assessment是独立业务评估；context.knowledge含version、scope、records，每条记录有id/title/tags/content。
需要提供7个公开函数：
1.observe(arguments,context)：返回context['observation']原始全部字段，不添加成功结论。
2.inspect_links(arguments,context)：返回version、step、nodes、links四字段。
3.inspect_services(arguments,context)：返回version、step、flows及assessment=context['assessment']。
4.list_routes(arguments,context)：遍历真实有向拓扑，为每条flow枚举从source到target的全部简单路径（包括故障/未启用的路径）。
 返回{'version':当前版本,'routes':{flow_id:[路径记录,...]}}。路径记录必须包含：
 path_id='+'.join(链路ID列表)、nodes（源至目的节点ID列表）、link_ids（有序链路ID列表）、
 operational（所有节点online且所有链路up和enabled）、bottleneck_capacity_mbps（路径链路最小容量）、
 disabled_links、failed_links、device_protocols（链路ID到协议映射）。不选择最佳路径、不返回处置答案。
5.retrieve_knowledge(arguments,context)：query字符串按空格分词，不区分大小写，与title、tags、content做子串匹配；
 返回{'version':知识版本,'scope':知识scope,'hits':[匹配的原记录,...]}，保持输入顺序、最多5条，无匹配则空列表。
6.service_operations(arguments,context)：arguments.services包含1至2条配置，每条flow_id必需、path_id和source_limit_mbps至少一个。
 path_id只能查list_routes的真实路径，不得把任意节点字符串当路径。每条依次转换path_id为
 {'op':'set_route','flow_id':实际flow_id,'route':路径link_ids}，若有source_limit_mbps则再添加
 {'op':'set_rate_limit','flow_id':实际flow_id,'limit_mbps':输入原数值}。返回操作列表，不选方案、不执行、不改变输入。
 未知业务、重复业务、未知path_id、无调整字段、额外字段要抛ValueError。数值范围和任务约束另由宿主执行器最终校验。
7.device_operations(arguments,context)：从真实links校验link_id存在且protocol为standard-v1；不匹配抛ValueError。
 返回单项列表[{'op':'configure_link','link_id':输入link_id,'enabled':输入布尔值,'channel':输入整数}]。
 enabled必须是bool且channel是1至11的int（bool不作为int）；不得改物理容量和up状态。
可使用for、递归辅助函数、列表/字典推导。允许内置int/float/str/bool/dict/list/set/tuple/len/min/max/sum/all/any/range/enumerate/sorted/zip/isinstance/ValueError/abs。
仅允许对象方法get/items/keys/values/append/extend/copy/lower/casefold/split/join/strip/add/remove/discard/issubset/pop；不使用其他方法。不允许while循环。
代码必须对任意有效的这类小拓扑工作；测试包含改变身份和拓扑，不能只针对某个案例。
"""


def invoke(python, source_path, function, arguments, context):
    process = subprocess.run([python, "-I", str(ROOT / "collab_lab" / "bundle_worker.py")],
        input=json.dumps({"code": Path(source_path).read_text(encoding="utf-8"), "function": function,
                          "arguments": arguments, "context": context}, ensure_ascii=True),
        capture_output=True, text=True, encoding="utf-8", timeout=6,
        creationflags=getattr(subprocess, "CREATE_NO_WINDOW", 0))
    try:
        result = json.loads(process.stdout)
    except ValueError:
        raise ContractError("业务工具进程未返回有效JSON")
    if not result.get("ok"):
        raise ContractError(result.get("error", "业务工具执行失败"))
    return result["value"]


def context_for(world, observation=None):
    book = read_json(ROOT / "knowledge" / "records.json")
    if observation is None:
        return {"observation": world.observation(), "assessment": world.assess(), "knowledge": book}
    metrics, task = observation["metrics"], observation["task"]
    voice = metrics.get("voice", {}).get("demand_satisfaction")
    video = metrics.get("video", {}).get("delivered_mbps", 0.)
    checks = {"voice_target_met": voice is not None and voice >= task["voice_min_satisfaction"] - 1e-9,
              "video_floor_met": video >= task["video_min_mbps"] - 1e-9}
    assessment = {"checks": checks, "objective_met": all(checks.values()), "metrics": copy.deepcopy(metrics),
                  "scope": "sampled sensor delivery; no long-term or physical-device guarantee"}
    return {"observation": observation, "assessment": assessment, "knowledge": book}


def verify_bundle(path, python):
    """Developer-owned oracles; no expected answers were sent in the author prompt."""
    cases = []
    def check(name, body):
        try:
            body()
            cases.append({"name": name, "passed": True})
        except Exception as error:
            cases.append({"name": name, "passed": False, "error": f"{type(error).__name__}: {error}"})
    def equal(actual, expected):
        if actual != expected:
            raise AssertionError("实际输出与独立契约不符：" + dumps({"actual": actual, "expected": expected})[:2000])
    def call(function, args, ctx):
        return invoke(python, path, function, args, ctx)
    world = NetworkWorld("degraded")
    ctx = context_for(world)
    for fn, expected in [("observe", ctx["observation"]),
                         ("inspect_links", {k:ctx["observation"][k] for k in ("version", "step", "nodes", "links")}),
                         ("inspect_services", {**{k:ctx["observation"][k] for k in ("version", "step", "flows")}, "assessment":ctx["assessment"]})]:
        check(fn + "_actual_state", lambda fn=fn, expected=expected: equal(call(fn, {}, ctx), expected))
    for scene in ("normal", "degraded", "adapter_gap", "no_resource"):
        def route_case(scene=scene):
            w = NetworkWorld(scene)
            result = call("list_routes", {}, context_for(w))
            equal({k:sorted(v,key=lambda p:p["path_id"]) for k,v in result["routes"].items()},
                  {k:sorted(v,key=lambda p:p["path_id"]) for k,v in reference_routes(w)["routes"].items()})
        check("all_paths_" + scene, route_case)
    def renamed_topology():
        w = NetworkWorld("adapter_gap")
        w.state["links"]["radio_z"] = w.state["links"].pop("backup")
        w.state["nodes"][1]["online"] = False
        w.state["flows"]["telemetry"] = copy.deepcopy(w.state["flows"]["voice"])
        actual = call("list_routes", {}, context_for(w))["routes"]
        equal({k:sorted(v,key=lambda p:p["path_id"]) for k,v in actual.items()},
              {k:sorted(v,key=lambda p:p["path_id"]) for k,v in reference_routes(w)["routes"].items()})
    check("changed_identity_node_failure_and_extra_flow", renamed_topology)
    services = [{"flow_id":"voice", "path_id":"access+backup"}, {"flow_id":"video", "path_id":"access+backup", "source_limit_mbps":3.}]
    expected_ops = [{"op":"set_route","flow_id":"voice","route":["access","backup"]},
                    {"op":"set_route","flow_id":"video","route":["access","backup"]},
                    {"op":"set_rate_limit","flow_id":"video","limit_mbps":3.}]
    check("service_translation", lambda: equal(call("service_operations", {"services":services}, ctx), expected_ops))
    def live_execution():
        w = NetworkWorld("degraded")
        ops = call("service_operations", {"services":services}, context_for(w))
        equal(w.apply(ops, 1)["assessment"]["objective_met"], True)
    check("translation_reaches_actual_executor", live_execution)
    invalids = [[{"flow_id":"voice"}], [{"flow_id":"missing","path_id":"primary"}],
                [{"flow_id":"voice","path_id":"field+relay+command"}],
                [{"flow_id":"voice","path_id":"primary","extra":True}],
                [{"flow_id":"voice","path_id":"primary"}]*2]
    for index, value in enumerate(invalids):
        def reject(value=value):
            try:
                call("service_operations", {"services":value}, ctx)
            except ContractError:
                return
            raise AssertionError("非法配置未拒绝")
        check("reject_invalid_services_" + str(index), reject)
    for args in ({"link_id":"primary","enabled":False,"channel":3}, {"link_id":"access","enabled":True,"channel":11}):
        check("device_"+args["link_id"], lambda args=args: equal(call("device_operations",args,ctx), [{"op":"configure_link",**args}]))
    def reject_protocol():
        try:
            call("device_operations", {"link_id":"backup","enabled":True,"channel":6}, context_for(NetworkWorld("adapter_gap")))
        except ContractError:
            return
        raise AssertionError("不得假装兼容新协议")
    check("real_protocol_gap_preserved", reject_protocol)
    for query, ids in (("RESCUE", ["K04"]), ("zzzz_unknown_marker", [])):
        def knowledge_case(query=query, ids=ids):
            r = call("retrieve_knowledge", {"query":query}, ctx)
            equal([row["id"] for row in r["hits"]], ids)
            equal(r["scope"],ctx["knowledge"]["scope"])
        check("knowledge_"+query, knowledge_case)
    return {"passed": all(x["passed"] for x in cases), "cases": cases,
            "source_sha256": hashlib.sha256(Path(path).read_bytes()).hexdigest(),
            "scope":"independent finite contract and simulation tests; no physical-device validation"}


class GeneratedToolchain:
    def __init__(self, config, directory):
        self.python = config["python"]
        origin = read_json(ROOT / "toolchain" / "current.json")
        source = ROOT / "toolchain" / "candidates" / origin["id"] / "tool.py"
        if not re.fullmatch(r"[a-f0-9]{12}", origin["id"]):
            raise ContractError("工具版本身份无效")
        data = source.read_bytes()
        if hashlib.sha256(data).hexdigest() != origin["source_sha256"]:
            raise ContractError("基础工具源码与发布哈希不一致")
        self.path = directory / "toolchain" / "tool.py"
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.path.write_bytes(data)
        self.manifest = origin
        write_json(self.path.parent / "manifest.json", origin)

    def call(self, function, arguments, world, observation=None):
        if hashlib.sha256(self.path.read_bytes()).hexdigest() != self.manifest["source_sha256"]:
            raise ContractError("会话固定的业务工具源码发生变化")
        return invoke(self.python, self.path, function, arguments, context_for(world, observation))


def forge_tools(config, providers, turn_budget=14):
    root = ROOT / "toolchain"
    job_id = uuid.uuid4().hex[:12]
    evidence = Evidence(root / "jobs" / job_id)
    allowed = {"write_bundle":"arguments.code写入完整模块源码，返回候选身份", "test_bundle":"arguments.candidate_id执行独立测试",
               "read_bundle":"arguments.candidate_id读取源码/测试", "publish_bundle":"arguments.candidate_id发布通过测试的版本",
               "finish_failure":"arguments.reason如实记录未能完成"}
    messages = [{"role":"system", "content":"你是Qwen业务工具制作Agent。按需求自主编写、调用真实测试并发布。失败可修订新候选；不得口头冒充完成。每轮返回JSON {action,arguments,reason}。工具："+dumps(allowed)},
                {"role":"user", "content":REQUIREMENTS}]
    outcome = None
    def candidate_path(ident):
        if not re.fullmatch(r"[a-f0-9]{12}", str(ident)):
            raise ContractError("无效候选身份")
        return root / "candidates" / ident
    for turn in range(turn_budget):
        answer = providers.call("cloud", messages, allowed, evidence)
        value, args = answer["parsed"], answer["parsed"]["arguments"]
        name = value["action"]
        messages.append({"role":"assistant", "content":answer["content"]})
        try:
            if name == "write_bundle":
                code = args.get("code")
                if not isinstance(code,str) or not code.strip():
                    raise ContractError("缺少源码字符串")
                ident = uuid.uuid4().hex[:12]
                path = candidate_path(ident)
                path.mkdir(parents=True)
                (path/"tool.py").write_text(code,encoding="utf-8")
                metadata = {"id":ident,"author_model":answer["model"],"author_call_id":answer["call_id"],
                            "author_job_id":job_id,"created":now(),"source":"qwen_original_response","functions":list(FUNCTIONS)}
                write_json(path/"descriptor.json",metadata)
                result = metadata
            elif name in ("test_bundle","read_bundle","publish_bundle"):
                ident = args["candidate_id"]
                path = candidate_path(ident)
                if name == "test_bundle":
                    result = verify_bundle(path/"tool.py",config["python"])
                    write_json(path/"test_result.json",result)
                elif name == "read_bundle":
                    result = {"code":(path/"tool.py").read_text(encoding="utf-8")}
                    if (path/"test_result.json").exists():
                        result["tests"] = read_json(path/"test_result.json")
                else:
                    tests = read_json(path/"test_result.json")
                    digest = hashlib.sha256((path/"tool.py").read_bytes()).hexdigest()
                    if not tests["passed"] or digest != tests["source_sha256"]:
                        raise ContractError("源码未通过独立测试或已改变")
                    result = {**read_json(path/"descriptor.json"),"source_sha256":digest,"published":now(),"contract_cases":len(tests["cases"])}
                    write_json(path/"published.json",result)
                    write_json(root/"current.json",result)
                    outcome = {"status":"published","manifest":result,"evidence_path":str(evidence.directory)}
            elif name == "finish_failure":
                result = args
                outcome = {"status":"unresolved","reason":args.get("reason"),"evidence_path":str(evidence.directory)}
            else:
                raise ContractError("不支持该制作工具")
            evidence.add("tool_result","toolmaker",{"action":name,"arguments":args if name!='write_bundle' else {"source_recorded_in_call":True},"result":result})
        except Exception as error:
            result = {"ok":False,"error":f"{type(error).__name__}: {error}"}
            evidence.add("tool_error","toolmaker",{"action":name,**result})
        messages.append({"role":"user","content":dumps({"tool":name,"result":result})})
        write_json(evidence.directory/"conversation.json",messages)
        if outcome:
            break
    outcome = outcome or {"status":"test_budget_exhausted","evidence_path":str(evidence.directory)}
    write_json(evidence.directory/"result.json",outcome)
    return outcome
