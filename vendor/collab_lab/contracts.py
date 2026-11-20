"""The same argument contracts are exposed to the agent and constrained generation."""


def obj(properties, required=(), additional=False):
    return {"type": "object", "properties": properties, "required": list(required), "additionalProperties": additional}


STRING, INTEGER = {"type": "string"}, {"type": "integer"}
OPERATION = {"oneOf": [
    obj({"op": {"enum": ["set_route"]}, "flow_id": STRING, "route": {"type": "array", "minItems": 1, "items": STRING}}, ["op", "flow_id", "route"]),
    obj({"op": {"enum": ["set_rate_limit"]}, "flow_id": STRING, "limit_mbps": {"type": "number"}}, ["op", "flow_id", "limit_mbps"]),
    obj({"op": {"enum": ["configure_link"]}, "link_id": STRING, "enabled": {"type": "boolean"}, "channel": INTEGER}, ["op", "link_id", "enabled"])]}
OPERATIONS = {"type": "array", "items": OPERATION, "minItems": 1, "maxItems": 12}
SERVICE = obj({"flow_id": STRING,
               "path_id": {"type": "string", "description": "list_routes返回的path_id，原样填写"},
               "source_limit_mbps": {"type": "number", "description": "业务源速率上限；不改变物理链路容量"}}, ["flow_id"])
SERVICES = {"type": "array", "items": SERVICE, "minItems": 1, "maxItems": 2}
ARGUMENTS = {
    'call_appliance_api':obj({'path':STRING,'method':{'enum':['GET','POST']},'body':{'type':'object','additionalProperties':True},'observation_id':STRING},['path']),
    'check_health':obj({'observation_id':STRING},['observation_id']),
    'read_cloud_status':obj({}), 'wait_for_cloud':obj({}),
    "read_sensor_adapter": obj({}), "observe": obj({}), "inspect_links": obj({}), "inspect_services": obj({}), "read_catalog": obj({}),
    "list_routes": obj({"observation_id":STRING},["observation_id"]),
    "simulate_services": obj({"observation_id": STRING,"services": SERVICES, "steps": {"type": "integer", "minimum": 1, "maximum": 30}}, ["services","observation_id"]),
    "apply_services": obj({"services": SERVICES, "observation_id": STRING}, ["services", "observation_id"]),
    "configure_device": obj({"link_id": STRING, "enabled": {"type": "boolean"}, "channel": INTEGER,
                             "observation_id": STRING}, ["link_id", "enabled", "channel", "observation_id"]),
    "retrieve_knowledge": obj({"query": STRING}, ["query"]),
    "simulate_plan": obj({"observation_id":STRING,"operations": OPERATIONS, "steps": {"type": "integer", "minimum": 1, "maximum": 30}}, ["operations","observation_id"]),
    "apply_plan": obj({"operations": OPERATIONS, "observation_id": STRING}, ["operations", "observation_id"]),
    "check_resource_feasibility": obj({"observation_id":STRING},["observation_id"]),
    "get_request_status": obj({"request_id":STRING},["request_id"]),
    "get_receipt": obj({"receipt_id": STRING}, ["receipt_id"]),
    "get_report_template": obj({}),
    "report_gap": obj({"report": {}}, additional=True),
    "request_cloud_plan": obj({"request": {}}, additional=True),
    "finish": obj({"observation_id":STRING,"status": {"enum": ["solved", "needs_resources", "unresolved"]}, "summary": STRING}, ["status","observation_id"])}
PLUGIN = obj({"input": obj({"link_id": STRING, "enabled": {"type": "boolean"}, "channel": INTEGER},
                           ["link_id", "enabled", "channel"]), "observation_id": STRING, "preview": {"type": "boolean"}},
             ["input", "observation_id"])
