"""Mesh —— 个人 AI 网格操作系统。

把三种资源统一到一张网里：

* **文件系统** —— 每个节点暴露若干命名 root（来自 linux-file-bridge 的路径围栏）
* **能力**     —— 每个节点可查询的 CPU/内存/工具/模型/root 清单（全新）
* **算力**     —— 每个节点若干可调度的槽位，跑 DAG 任务（来自 ai-swarm 的调度器）

统一寻址 ``mesh://node/env:root/path``，数据按块级增量传输，全程零第三方依赖。

模块分工：

============  ==========================================================
``address``   统一寻址：解析、规范化、通配匹配
``delta``     块级增量传输：Adler-32 滚动校验 + sha256，rsync 式差分
``capability``能力注册表：本机探测、调度打分、节点清单
``queue``     持久作业队列：SQLite WAL + 租约 + 崩溃恢复
``task``      任务图：DAG 校验、批次、写作用域
``wire``      零依赖 HTTP + JSON-RPC（客户端与服务端）
``agent``     节点 agent：路径围栏、只读 root、作用域强制
``hub``       控制面：心跳、派发、失败重试、面板
``cli``       命令行：node / hub / submit / status / doctor
============  ==========================================================

最省事的起法（两台机器，同一个局域网）::

    # 手机（Termux + proot）：把工作目录暴露出去
    mesh node --id 手机 --root work=~/proj --token 123 --port 8765

    # 任意一台常开的机器：开始调度，手机浏览器打开面板看全程
    mesh hub --db mesh.db --node 手机=http://手机IP:8765 \\
             --node-token 123 --port 8788 --token 123
"""

from __future__ import annotations

from . import address, agent, capability, delta, hub, queue, task, wire

__version__ = "0.1.0"
PROTO = 1
NAME = "mesh"

__all__ = [
    "__version__", "PROTO", "NAME",
    "address", "agent", "capability", "delta", "hub", "queue", "task", "wire",
    "MeshAddress", "parse", "matches",
    "Capability", "Registry", "probe", "score",
    "Delta", "diff", "patch",
    "Job", "Queue",
    "TaskSpec", "Plan", "check_scope",
    "Node", "serve", "jail",
    "Hub", "HubError", "NodeRef",
    "Client", "Server", "WireError",
]

# 常用名字提到顶层，方便 `from mesh import parse, diff, Hub`
MeshAddress = address.MeshAddress
parse = address.parse
matches = address.matches

Capability = capability.Capability
Registry = capability.Registry
probe = capability.probe
score = capability.score

Delta = delta.Delta
diff = delta.diff
patch = delta.patch

Job = queue.Job
Queue = queue.Queue

TaskSpec = task.TaskSpec
Plan = task.Plan
check_scope = task.check_scope

Node = agent.Node
serve = agent.serve
jail = agent.jail

Hub = hub.Hub
HubError = hub.HubError
NodeRef = hub.NodeRef

Client = wire.Client
Server = wire.Server
WireError = wire.WireError
