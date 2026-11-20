"""Fluid-queue network simulator. No LLM or scene-label-driven controller."""
import copy
import math
import threading
from .common import digest, now


class ContractError(ValueError):
    pass


SCENARIOS = {
    "normal": "正常通信",
    "degraded": "主链路容量下降",
    "congestion": "视频业务增量",
    "adapter_gap": "备用设备接口不兼容",
    "no_resource": "全部回传资源不可用",
}


def initial_state(scene="normal"):
    if scene not in SCENARIOS:
        raise ValueError("未知场景")
    state = {
        "version": 1, "environment_revision": 1, "step": 0,
        "nodes": [{"id": "field", "name": "现场一体机", "online": True, "battery_soc": .86, "x": 80, "y": 180},
                  {"id": "relay", "name": "便携中继", "online": True, "battery_soc": .73, "x": 320, "y": 85},
                  {"id": "command", "name": "指挥端", "online": True, "battery_soc": 1., "x": 580, "y": 180}],
        "links": {
            "primary": {"source": "field", "target": "command", "capacity_mbps": 8., "up": True,
                        "enabled": True, "channel": 6, "protocol": "standard-v1"},
            "access": {"source": "field", "target": "relay", "capacity_mbps": 8., "up": True,
                       "enabled": True, "channel": 6, "protocol": "standard-v1"},
            "backup": {"source": "relay", "target": "command", "capacity_mbps": 6., "up": True,
                       "enabled": True, "channel": 6, "protocol": "standard-v1"}},
        "flows": {
            "voice": {"name": "抢修语音", "source": "field", "target": "command", "requested_mbps": .25,
                      "limit_mbps": .25, "route": ["primary"], "queue_mbit": 0.},
            "video": {"name": "现场视频", "source": "field", "target": "command", "requested_mbps": 5.,
                      "limit_mbps": 5., "route": ["primary"], "queue_mbit": 0.}},
        "metrics": {}, "history": []}
    if scene in ("degraded", "adapter_gap"):
        state["links"]["primary"]["capacity_mbps"] = .8
    if scene == "congestion":
        state["flows"]["video"]["requested_mbps"] = 12.
        state["flows"]["video"]["limit_mbps"] = 12.
    if scene == "adapter_gap":
        state["links"]["backup"].update(enabled=False, protocol="rescue-v2")
    if scene == "no_resource":
        state["links"]["primary"]["up"] = False
        state["links"]["backup"]["up"] = False
    return state


DEFAULT_TASK = {
    "instruction": "优先保障抢修语音，尽量保持视频回传。调查现场后自行选择处置；已有工具不能解决才报告能力缺口。",
    "voice_min_satisfaction": .95, "video_min_mbps": 2.,
    "allowed_video_degradation": True}


def finite_number(value, minimum=0., maximum=1000.):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContractError("参数必须为有限数值")
    value = float(value)
    if not minimum <= value <= maximum:
        raise ContractError(f"参数超出范围[{minimum}, {maximum}]")
    return value


class NetworkWorld:
    def __init__(self, scene="normal", task=None, state=None):
        self.lock = threading.RLock()
        self.state = copy.deepcopy(state if state is not None else initial_state(scene))
        self.state.setdefault('environment_revision',1)
        self.task = copy.deepcopy(task or DEFAULT_TASK)
        if not self.state["metrics"]:
            self.tick()

    def snapshot(self):
        with self.lock:
            return copy.deepcopy(self.state)

    def observation(self):
        with self.lock:
            data = {k: copy.deepcopy(self.state[k]) for k in ("version", "environment_revision", "step", "nodes", "links", "flows", "metrics")}
            data["observed_at"] = now()
            data["task"] = copy.deepcopy(self.task)
            data["measurement_model"] = "1秒步长流体队列；不包含无线信道物理、包级端到端时延或真实设备"
            data["control_semantics"] = {"up":"外部物理可用状态；配置工具不能改变或修复",
                "enabled":"设备管理开关，不能覆盖up=false的物理故障",
                "channel":"首版只保存信道配置，不模拟干扰消除；更换信道不会改变up或容量",
                "capacity_mbps":"外部可用容量，端侧工具不能增加物理容量",
                "source_limit_mbps":"可调整的应用源速率上限；与业务需求分开记录"}
            return data

    def _path_valid(self, flow, route):
        if not isinstance(route, list) or not route or len(route) > len(self.state["links"]):
            return False
        if len(set(route)) != len(route):
            return False
        current = flow["source"]
        for ident in route:
            link = self.state["links"].get(ident)
            if not link or link["source"] != current:
                return False
            current = link["target"]
        return current == flow["target"]

    def _operational(self, route):
        online = {n["id"]: n["online"] for n in self.state["nodes"]}
        return all(self.state["links"][i]["up"] and self.state["links"][i]["enabled"]
                   and online[self.state["links"][i]["source"]]
                   and online[self.state["links"][i]["target"]] for i in route)

    def tick(self):
        with self.lock:
            flows, links = self.state["flows"], self.state["links"]
            offered = {i: min(f["requested_mbps"], f["limit_mbps"]) for i, f in flows.items()}
            for i, flow in flows.items():
                flow["queue_mbit"] += offered[i]
            # Each link shares its capacity proportionally between admitted flow rates.
            # Source caps represent permitted application bitrate reduction, not packet deletion.
            load = {i: sum(offered[f] for f, flow in flows.items() if i in flow["route"]
                           and self._operational(flow["route"])) for i in links}
            measured = {}
            for ident, flow in flows.items():
                delivery = 0.
                if self._path_valid(flow, flow["route"]) and self._operational(flow["route"]):
                    ratio = min([1.] + [links[i]["capacity_mbps"] / load[i] if load[i] else 1.
                                          for i in flow["route"]])
                    delivery = min(flow["queue_mbit"], offered[ident] * ratio)
                flow["queue_mbit"] = max(0., flow["queue_mbit"] - delivery)
                measured[ident] = {"admitted_mbps": offered[ident], "delivered_mbps": delivery,
                                   "requested_mbps": flow["requested_mbps"], "queue_mbit": flow["queue_mbit"],
                                   "demand_satisfaction": min(1., delivery / flow["requested_mbps"]) if flow["requested_mbps"] else None}
            self.state["step"] += 1
            self.state["metrics"] = measured
            row = {"step": self.state["step"], "version": self.state["version"], "flows": copy.deepcopy(measured)}
            self.state["history"].append(row)
            self.state["history"] = self.state["history"][-300:]
            return row

    def assess(self):
        with self.lock:
            metrics = copy.deepcopy(self.state["metrics"])
            voice = metrics.get("voice", {}).get("demand_satisfaction")
            video = metrics.get("video", {}).get("delivered_mbps", 0.)
            checks = {"voice_target_met": voice is not None and voice >= self.task["voice_min_satisfaction"] - 1e-9,
                      "video_floor_met": video >= self.task["video_min_mbps"] - 1e-9}
            return {"checks": checks, "objective_met": all(checks.values()), "metrics": metrics,
                    "scope": "current simulated delivery; no long-term or physical-device guarantee"}

    def _apply_one(self, operation, plugin_protocol=None):
        op = operation.get("op")
        if op == "set_route":
            flow = self.state["flows"].get(operation.get("flow_id"))
            if not flow or not self._path_valid(flow, operation.get("route")):
                raise ContractError("路由必须构成从业务源到目的的有向路径")
            if not self._operational(operation["route"]):
                raise ContractError("路径包含未启用、故障或节点离线的链路")
            flow["route"] = list(operation["route"])
        elif op == "set_rate_limit":
            ident = operation.get("flow_id")
            if ident not in self.state["flows"]:
                raise ContractError("未知业务")
            cap = finite_number(operation.get("limit_mbps"), 0., 100.)
            if ident == "voice" and cap < self.state["flows"][ident]["requested_mbps"] * self.task["voice_min_satisfaction"]:
                raise ContractError("任务约束不允许将语音源速率限制到保障要求以下")
            if ident == "video" and cap < self.task["video_min_mbps"]:
                raise ContractError("任务约束要求保留最低视频源速率")
            self.state["flows"][ident]["limit_mbps"] = cap
        elif op in ("configure_link", "device_command"):
            ident = operation.get("link_id")
            if ident not in self.state["links"]:
                raise ContractError("未知链路")
            link = self.state["links"][ident]
            if op == "configure_link":
                if link["protocol"] != "standard-v1":
                    raise ContractError("现有配置工具只支持standard-v1；设备实际接口为" + link["protocol"])
                enabled, channel = operation.get("enabled"), operation.get("channel", link["channel"])
            else:
                if plugin_protocol != link["protocol"] or operation.get("protocol") != plugin_protocol:
                    raise ContractError("device_command仅允许通过匹配协议的已发布工具执行")
                payload = operation.get("payload", {})
                if payload.get("admin_state") not in ("up", "down"):
                    raise ContractError("rescue-v2需要admin_state=up/down")
                enabled, channel = payload["admin_state"] == "up", payload.get("rf_channel")
            if type(enabled) is not bool or type(channel) is not int or not 1 <= channel <= 11:
                raise ContractError("enabled须为布尔值，channel须为1至11的整数")
            link.update(enabled=enabled, channel=channel)
        else:
            raise ContractError("不支持的操作：" + str(op))

    def apply(self, operations, expected_version, plugin_protocol=None):
        with self.lock:
            if type(expected_version) is not int or expected_version != self.state["version"]:
                raise ContractError("状态版本已变化，请读取当前状态后重新决策")
            if not isinstance(operations, list) or not 1 <= len(operations) <= 12:
                raise ContractError("计划须包含1至12项操作")
            before = copy.deepcopy(self.state)
            try:
                for operation in operations:
                    self._apply_one(operation, plugin_protocol)
                if self.state==before:
                    raise ContractError('配置没有变化；请依据实际效果决定下一动作，不重复同一配置')
                self.state["version"] += 1
                self.tick()
            except Exception:
                self.state = before
                raise
            return {"ok": True, "previous_version": before["version"], "version": self.state["version"],
                    "operations": copy.deepcopy(operations), "assessment": self.assess(), "scope": "live_simulation"}

    def simulate(self, operations, steps=5, plugin_protocol=None):
        if type(steps) is not int or not 1 <= steps <= 30:
            raise ContractError("推演步数须为1至30")
        snapshot = self.snapshot()
        twin = NetworkWorld(task=self.task, state=snapshot)
        twin.apply(operations, snapshot["version"], plugin_protocol)
        for _ in range(steps - 1):
            twin.tick()
        return {"source_version": snapshot["version"], "source_step": snapshot["step"],
                "snapshot_sha256": digest(snapshot), "predicted": twin.assess(), "steps": steps,
                "scope": "counterfactual_snapshot_only", "live_state_changed": False}

    def perturb(self, changes):
        with self.lock:
            candidate = copy.deepcopy(self.state)
            for ident, update in changes.get("links", {}).items():
                if ident not in candidate["links"]:
                    raise ContractError("未知链路")
                for key, value in update.items():
                    if key == "capacity_mbps":
                        value = finite_number(value, 0., 100.)
                    elif key == "up":
                        if type(value) is not bool:
                            raise ContractError("up须为布尔值")
                    else:
                        raise ContractError("环境控件只修改容量和故障状态")
                    candidate["links"][ident][key] = value
            for ident, update in changes.get("flows", {}).items():
                if ident not in candidate["flows"] or set(update) != {"requested_mbps"}:
                    raise ContractError("仅支持业务需求变更")
                candidate["flows"][ident]["requested_mbps"] = finite_number(update["requested_mbps"], .01, 100.)
            if candidate==self.state:
                return self.observation()
            candidate["environment_revision"] = candidate.get("environment_revision",1)+1
            self.state = candidate
            self.tick()
            return self.observation()
