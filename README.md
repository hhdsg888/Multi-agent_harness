# Multi-agent harness

当前版本：r6。面向通信现场的中心协调与端侧多智能体协同架构。

A兼任中心协调和本端处置，B独立观察、审议并执行本端配置。当前通信现场由仿真执行服务承载；模型推理通过Ollama接口完成。

## 架构组成

- `dual_lab/`：智能体、模型适配、消息客户端、协议、任务与状态账本、本端执行服务，以及该版本需要的运行模块。
- `vendor/`：固定版本的1.1.2基础库、知识/策略资料及运行资源；保持原依赖快照。
- `initialize.py`：创建服务运行需要的本地认证配置，不修改源码或操作远程机器。
- `VERSION.json`、`REVISION_HISTORY.json`、`HISTORY_MAP.json`：版本来源与核心源码校验、新旧提交对应关系；后两项见主分支。

本仓库只发布架构运行代码及必要依赖。SSH控制、远程部署、代码迁移/上传/清理、实验驱动、审计、一次性检查和本地操作指令均不包含在公开版本中。

## 基本运行

需要Python 3.10及以上，并自行准备Ollama模型服务及对应的原始`gemma4-e2b:latest`模型。仓库不包含权重、凭据或私人实验数据。

```sh
python initialize.py
python -m dual_lab.service --port 18933
```

在A、B各自的主机和终端启动智能体。`--url`指向同一个可达的执行服务，`--model-url`指向本端模型服务；认证配置须与执行服务一致。连接方式由部署环境提供。

```sh
python -m dual_lab.agent --node A --url http://127.0.0.1:18933 --model-url http://127.0.0.1:11434
python -m dual_lab.agent --node B --url http://127.0.0.1:18933 --model-url http://127.0.0.1:11434
```

任务通过执行服务的管理接口创建。例如，在执行服务所在环境用本地管理凭据提交该版本的一个正常场景：

```python
import json
from pathlib import Path
from dual_lab.client import Client

auth = json.loads(Path("auth.local.json").read_text())
client = Client("http://127.0.0.1:18933", auth["admin"])
job = client.call({'op': 'create', 'fixture': 'T00'})
print(job)
```

## 版本回滚

r1—r15按顺序保留同名标签。清理仓库范围时只移除了非架构材料，各版本保留的运行源码与清理前逐字节一致。r13原本是依据留存材料恢复的版本，这一来源限制仍保留。

```sh
git switch --detach r6
```

新旧提交哈希见主分支`HISTORY_MAP.json`。既有实验原始记录不改写，可据映射定位相同核心源码。后续只提交架构改动，不上传开发操作脚本或制作本地历史源码副本。
