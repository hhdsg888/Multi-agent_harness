"""Topology inspection and exact service-to-device translation; no plan selection."""
from .environment import ContractError


def routes(world):
    state = world.snapshot()
    online = {n["id"]: n["online"] for n in state["nodes"]}
    result = {}
    for flow_id, flow in state["flows"].items():
        found = []
        def visit(node, node_ids, link_ids):
            if node == flow["target"]:
                links = [state["links"][i] for i in link_ids]
                found.append({"path_id": "+".join(link_ids), "nodes": node_ids, "link_ids": link_ids,
                              "operational": all(l["up"] and l["enabled"] for l in links) and all(online[i] for i in node_ids),
                              "bottleneck_capacity_mbps": min(l["capacity_mbps"] for l in links),
                              "disabled_links": [i for i in link_ids if not state["links"][i]["enabled"]],
                              "failed_links": [i for i in link_ids if not state["links"][i]["up"]],
                              "device_protocols": {i: state["links"][i]["protocol"] for i in link_ids}})
                return
            for ident, link in state["links"].items():
                if link["source"] == node and link["target"] not in node_ids:
                    visit(link["target"], node_ids + [link["target"]], link_ids + [ident])
        visit(flow["source"], [flow["source"]], [])
        result[flow_id] = found
    return {"version": state["version"], "routes": result,
            "notes": "列出拓扑中全部简单有向路径，包括不可用路径；容量是共享上限，不是保障速率。工具不选择方案。"}


def service_operations(world, services):
    if not isinstance(services, list) or not 1 <= len(services) <= len(world.state["flows"]):
        raise ContractError("services须为1至2项业务配置")
    options, operations, seen = routes(world)["routes"], [], set()
    for service in services:
        if not isinstance(service, dict) or set(service) - {"flow_id", "path_id", "source_limit_mbps"}:
            raise ContractError("业务配置字段仅限flow_id、path_id、source_limit_mbps")
        flow_id = service.get("flow_id")
        if flow_id not in options or flow_id in seen:
            raise ContractError("未知或重复业务")
        seen.add(flow_id)
        if "path_id" not in service and "source_limit_mbps" not in service:
            raise ContractError("请提供path_id或source_limit_mbps")
        if "path_id" in service:
            selected = next((p for p in options[flow_id] if p["path_id"] == service["path_id"]), None)
            if not selected:
                raise ContractError("未知path_id，请用list_routes查询并原样填写")
            operations.append({"op": "set_route", "flow_id": flow_id, "route": selected["link_ids"]})
        if "source_limit_mbps" in service:
            operations.append({"op": "set_rate_limit", "flow_id": flow_id, "limit_mbps": service["source_limit_mbps"]})
    return operations
