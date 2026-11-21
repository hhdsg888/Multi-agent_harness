# Multi-agent harness

当前版本：r17。面向通信现场的中心协调与端侧多智能体协同架构。

A兼任中心协调和本端处置，B独立观察、审议并执行本端配置。当前通信现场由仿真执行服务承载；模型推理通过Ollama接口完成。

## 架构组成

- `dual_lab/`：智能体、模型适配、消息客户端、协议、任务与状态账本、本端执行服务，以及该版本需要的运行模块。
- `vendor/`：固定版本的1.1.2基础库、知识/策略资料及运行资源；保持原依赖快照。
- `initialize.py`：创建服务运行需要的本地认证配置，不修改源码或操作远程机器。
- `VERSION.json`、`REVISION_HISTORY.json`、`HISTORY_MAP.json`：版本来源与核心源码校验、新旧提交对应关系；后两项见主分支。

本仓库只发布架构运行代码及必要依赖。SSH控制、远程部署、代码迁移/上传/清理、实验驱动、审计、一次性检查和本地操作指令均不包含在公开版本中。

## 当前交互规则

模型可自主调查、正常重试、提交多个原生工具调用和使用本端系统文件。两端共享1800秒窗口，任务卡只作责任提示。

r16将倒计时与稳定任务身份分开，按原请求上下文保存实际工具回执。上下文变化不丢弃回执，也不把旧回执算到无关的新任务上。

本端明确退出会向可达对端发送事实通知，保留未执行承诺；通知不会被当作执行回执，也不会单独反复激活已结束的任务。未解决或资源不足结案可以如实报告观测缺失、过期；solved仍要求有效观测、已接受的本端执行，以及适用时双方执行确认后的复采。

r17使用结构化动作数组提交模型决定，例如 `{"actions":[{"action":"read_sensor_adapter","arguments":{}}]}`。数组允许多个动作，工具名和参数契约保持不变；不能继续时由模型明确提交finish及其选择的状态。每轮突出当前激活的真实结案接收状态，历史solved不代表当前任务结束。正文和thinking不被提取成动作，格式错误返回事实回执。旧r15/r16继续使用原生工具提交。

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
job = client.call({'op': 'create', 'fixture': 'T00', 'change_policy': 'at_start', 'protocol_mode': 'structured', 'event_reactivation': True, 'task_dispatch': True, 'task_revision': 'r17'})
print(job)
```

## 版本回滚

r1—r17按顺序保留同名标签。历史范围清理仅移除了非架构材料，r1—r15保留的运行源码与清理前逐字节一致。r13原本是依据留存材料恢复的版本，这一来源限制仍保留。

```sh
git switch --detach r15
```

新旧提交哈希见主分支`HISTORY_MAP.json`。既有实验原始记录不改写，可据映射定位相同核心源码。后续只提交架构改动，不上传开发操作脚本或制作本地历史源码副本。
