import copy
import hashlib
import json
import re
import subprocess
import uuid
from .common import ROOT, read_json, write_json, now
from .environment import NetworkWorld, ContractError
from .capabilities import NETWORK,DIAGNOSTIC,PROTOCOLS,declared_capabilities,input_schema,expected_operations,diagnostic_truth

DEVICE_API = {
    "scope": "模拟设备API；仅用于工具制作机制验证",
    "protocol": "rescue-v2",
    "interface": "返回device_command操作，真实执行由宿主执行器校验",
    "operation_contract": {"op": "device_command", "link_id": "与arguments.link_id一致",
                           "protocol": "rescue-v2", "payload": {"admin_state": "enabled为true时up，否则down",
                                                                    "rf_channel": "与arguments.channel一致，整数1..11"}},
    "candidate_contract": "唯一函数def build_operations(arguments, device)，返回操作列表。无import/文件/网络/循环。"
                          "输入arguments包含link_id、enabled(bool)、channel(int)；device包含真实links元数据。",
    "input_schema_example": {"type": "object", "properties": {"link_id": {"type": "string"},
        "enabled": {"type": "boolean"}, "channel": {"type": "integer"}}, "required": ["link_id", "enabled", "channel"]}}


class CandidateStore:
    def __init__(self, directory, python):
        self.directory, self.python = directory, python
        self.published = {}

    def path(self, candidate_id):
        if not re.fullmatch(r"[a-f0-9]{12}", str(candidate_id)):
            raise ContractError("候选身份无效")
        return self.directory / candidate_id

    def create(self, arguments):
        name = arguments.get("name", "")
        if not re.fullmatch(r"[a-z][a-z0-9_]{2,47}", name):
            raise ContractError("工具名须为3至48位小写字母/数字/下划线")
        protocol=arguments.get('protocol')
        if protocol not in PROTOCOLS:raise ContractError('不支持的能力契约')
        capabilities=declared_capabilities(protocol,arguments.get('capabilities'))
        code = arguments.get("code")
        if not isinstance(code, str) or not code.strip():
            raise ContractError("缺少真实候选源码")
        schema = arguments.get("input_schema", input_schema(protocol))
        if not isinstance(schema, dict) or schema.get("type") != "object":
            raise ContractError("input_schema须为对象schema")
        ident = uuid.uuid4().hex[:12]
        path = self.path(ident)
        path.mkdir(parents=True)
        descriptor = {"id": ident, "name": name, "description": str(arguments.get("description", "rescue-v2设备适配")),
                      "protocol": protocol, "input_schema": schema, "capabilities":capabilities,"created": now(), "tested": False}
        descriptor['origin_action_id']=arguments.get('_runtime_action_id')
        (path / "tool.py").write_text(code, encoding="utf-8")
        write_json(path / "descriptor.json", descriptor)
        return descriptor

    def descriptor(self, ident):
        return read_json(self.path(ident) / "descriptor.json")

    def execute(self, ident, arguments, device, expected_sha256=None):
        descriptor=self.descriptor(ident);protocol=descriptor['protocol']
        expected=expected_operations(arguments,device,descriptor['capabilities']) if protocol==NETWORK else None
        if protocol==DIAGNOSTIC and arguments:raise ContractError('诊断工具输入应为空对象')
        source = (self.path(ident) / "tool.py").read_bytes()
        if expected_sha256 and hashlib.sha256(source).hexdigest()!=expected_sha256:
            from .runtime import RuntimeFault
            raise RuntimeFault('REPOSITORY_INTEGRITY','会话源码与仓库固定版本不一致，拒绝执行')
        published = self.path(ident) / "published.json"
        if published.exists() and hashlib.sha256(source).hexdigest() != read_json(published)["source_sha256"]:
            raise ContractError("已发布工具源码发生变化，拒绝执行未经验证的内容")
        code = source.decode("utf-8")
        payload = {"code": code, "arguments": arguments, "device": device}
        worker='plugin_worker.py' if protocol=='rescue-v2' else 'capability_worker.py'
        from .process_limits import run_candidate
        process = run_candidate([self.python, '-I', str(ROOT/'collab_lab'/worker)],payload,self.path(ident))
        try:
            result = json.loads(process.stdout)
        except ValueError:
            raise ContractError("候选工作进程未返回有效JSON")
        if not result.get("ok"):
            raise ContractError(result.get("error", "候选执行失败"))
        operations = result['operations']
        if protocol==NETWORK:
            if operations!=expected:raise ContractError('工具输出不符合输入或声明能力范围')
            return operations
        if protocol==DIAGNOSTIC:
            truth=diagnostic_truth(device)
            if not isinstance(operations,dict) or operations!=truth or any(type(v) is not bool for v in operations.values()):raise ContractError('诊断输出与独立实测事实不符')
            return operations
        expected = {'op': 'device_command', 'link_id': arguments.get('link_id'), 'protocol': 'rescue-v2',
                    'payload': {'admin_state': 'up' if arguments.get('enabled') is True else 'down',
                                'rf_channel': arguments.get('channel')}}
        if operations != [expected] or type(arguments.get('enabled')) is not bool or type(arguments.get('channel')) is not int:
            raise ContractError('rescue-v2工具只能按输入配置所请求设备的enabled/channel，不允许额外操作')
        return operations

    def import_artifact(self, artifact):
        descriptor = copy.deepcopy(artifact['descriptor'])
        ident = descriptor['id']
        path = self.path(ident)
        if path.exists():
            raise ContractError('会话候选身份冲突，拒绝覆盖')
        path.mkdir(parents=True)
        (path/'tool.py').write_bytes(artifact['source'])
        descriptor.update(repository_version=ident, loaded_from_repository=True)
        write_json(path/'descriptor.json', descriptor)
        write_json(path/'test_result.json', artifact['tests'])
        write_json(path/'published.json', descriptor)
        self.published[descriptor['name']] = descriptor
        return descriptor

    def test(self, ident):
        descriptor = self.descriptor(ident)
        source = (self.path(ident) / "tool.py").read_bytes()
        cases = []
        if descriptor['protocol']!='rescue-v2':return self.test_capability(ident,descriptor,source)
        # Independent interface cases: enable/disable and different channels/identities.
        for enabled, channel, link_id in [(True, 2, "backup"), (False, 9, "backup"), (True, 11, "secondary")]:
            world = NetworkWorld("adapter_gap")
            if link_id != "backup":
                world.state["links"][link_id] = copy.deepcopy(world.state["links"]["backup"])
            args = {"link_id": link_id, "enabled": enabled, "channel": channel}
            try:
                operations = self.execute(ident, args, world.observation())
                if len(operations) != 1 or operations[0].get("link_id") != link_id:
                    raise ContractError("适配输出必须只修改请求设备")
                before = world.snapshot()
                receipt = world.apply(operations, before["version"], plugin_protocol=descriptor["protocol"])
                actual = world.state["links"][link_id]
                if actual["enabled"] is not enabled or actual["channel"] != channel:
                    raise ContractError("执行结果未遵循输入enabled/channel")
                if any(world.state["links"][i] != before["links"][i] for i in before["links"] if i != link_id):
                    raise ContractError("修改了非请求设备")
                cases.append({"arguments": args, "passed": True, "receipt": receipt})
            except Exception as error:
                cases.append({"arguments": args, "passed": False, "error": str(error)})
        result = {"candidate_id": ident, "passed": all(row["passed"] for row in cases), "cases": cases,
                  "source_sha256": hashlib.sha256(source).hexdigest(), "scope": "independent simulated interface contract tests"}
        descriptor.update(tested=result["passed"], tested_sha256=result["source_sha256"])
        write_json(self.path(ident) / "test_result.json", result)
        write_json(self.path(ident) / "descriptor.json", descriptor)
        return result

    def test_capability(self,ident,descriptor,source):
        cases=[];protocol=descriptor['protocol'];inputs=[]
        if protocol==DIAGNOSTIC:
            inputs=[('normal',{}),('degraded',{}),('no_resource',{})]
        else:
            for cap in descriptor['capabilities']:
                if cap=='set_route':inputs += [('normal',{'services':[{'flow_id':'video','route':['access','backup']}]}),('normal',{'services':[{'flow_id':'voice','route':['access','backup']}]})]
                elif cap=='set_rate_limit':inputs += [('normal',{'services':[{'flow_id':'video','source_limit_mbps':3}]}),('normal',{'services':[{'flow_id':'video','source_limit_mbps':2.5}]})]
                else:inputs += [('normal',{'devices':[{'link_id':'backup','enabled':False,'channel':2}]}),('normal',{'devices':[{'link_id':'access','enabled':True,'channel':9}]})]
            combined={}
            services={}
            if 'set_route' in descriptor['capabilities']:services.update(flow_id='video',route=['access','backup'])
            if 'set_rate_limit' in descriptor['capabilities']:services.update(flow_id='video',source_limit_mbps=2)
            if services:combined['services']=[services]
            if 'configure_link' in descriptor['capabilities']:combined['devices']=[{'link_id':'primary','enabled':False,'channel':4}]
            inputs.append(('normal',combined))
        for scene,args in inputs:
            world=NetworkWorld(scene);before=world.snapshot()
            try:
                value=self.execute(ident,args,world.observation())
                if protocol==DIAGNOSTIC:
                    if value!=diagnostic_truth(world.observation()) or world.snapshot()!=before:raise ContractError('只读诊断失败')
                else:
                    expected=expected_operations(args,world.observation(),descriptor['capabilities'])
                    if value!=expected:raise ContractError('独立操作预言不符')
                    world.apply(value,before['version'])
                cases.append({'arguments':args,'passed':True})
            except Exception as error:cases.append({'arguments':args,'passed':False,'error':str(error)})
        result={'candidate_id':ident,'passed':all(r['passed'] for r in cases),'cases':cases,
            'source_sha256':hashlib.sha256(source).hexdigest(),'scope':'independent capability contract tests'}
        descriptor.update(tested=result['passed'],tested_sha256=result['source_sha256'])
        write_json(self.path(ident)/'descriptor.json',descriptor);write_json(self.path(ident)/'test_result.json',result)
        return result

    def publish(self, ident, reserved_names):
        descriptor = self.descriptor(ident)
        current = hashlib.sha256((self.path(ident) / "tool.py").read_bytes()).hexdigest()
        if not descriptor.get("tested") or current != descriptor.get("tested_sha256"):
            raise ContractError("候选尚未测试通过或源码已改变")
        if descriptor["name"] in reserved_names:
            raise ContractError("工具名已存在")
        descriptor.update(published_at=now(), source_sha256=current)
        self.published[descriptor["name"]] = descriptor
        write_json(self.path(ident) / "published.json", descriptor)
        return descriptor
