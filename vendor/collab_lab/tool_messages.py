"""Explain capability publication without selecting or executing an action."""


def cloud_delivery_message(packet):
    message={"type":"cloud_result","data":packet}
    if packet.get("type")=="tool_published":
        message["resume_context"]={"status":"published_not_applied", "published_tool_name":packet["published_tool"]["name"],
            "meaning":"新增执行能力已发布，原现场任务等待端侧继续决策。发布不代表工具已执行或业务已恢复。",
            "capability_update":"历史configure_device只支持standard-v1的错误仍成立，但它不能证明当前新增工具也不可用。以当前工具目录为准。",
            "decision_scope":"自主判断新能力是否适用；可观察、查目录、推演、调用或有依据地结束，不要求必须采用新工具。"}
    return message
