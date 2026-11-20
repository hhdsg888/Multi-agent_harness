import os
import sys
from pathlib import Path
from .common import ROOT, read_json


def load_config():
    config = read_json(ROOT / "config.json")
    local = ROOT / "config.local.json"
    if local.exists():
        config.update(read_json(local))
    for name in ('no_progress_limit','max_task_decisions','max_cloud_decisions'):
        if type(config.get(name,60)) is not int or config.get(name,60)<2:raise ValueError(name+'须为至少2的整数')
    if config.get('sensor_profile','legacy-fluid') not in ('legacy-fluid','appliance-mock','appliance-live'):raise ValueError('未知sensor_profile')
    config["python"] = config.get("python") or sys.executable
    if config.get("edge_backend") != "gemma" or config.get("tool_backend") != "qwen":
        raise ValueError("当前提交版使用Gemma端侧与Qwen业务工具；角色对比在实验版进行")
    config.setdefault('tool_repository_directory', 'tool_repository')
    for name in ("rag_pack", "strategy_pack", "report_template_file", "tool_repository_directory", "experience_directory", "runs_directory"):
        if config.get(name):
            config[name] = str((ROOT / config[name]).resolve())
    source = os.environ.get("COLLAB_MODELS_CONFIG", config.get("models_config_source"))
    if source:
        path = (ROOT / source).resolve()
        if not path.exists():
            raise ValueError("请复制models.example.json为models.local.json并配置模型，或设置COLLAB_MODELS_CONFIG")
        models = read_json(path)
        base = path.parent
    else:
        models, base = config.get("models", {}), ROOT
    config["models"] = {"edge": dict(models.get("gemma", models.get("edge", {}))),
                        "cloud": dict(models.get("qwen", models.get("cloud", {})))}
    config["models"]["edge"]["model"] = config["edge_model"]
    for role in ("edge", "cloud"):
        connection = config["models"][role]
        if not connection.get("base_url") or not connection.get("model"):
            raise ValueError(f"缺少{role}模型连接配置")
        if connection.get("private_directory"):
            connection["private_directory"] = str((base / connection["private_directory"]).resolve())
    return config
