# 示例

| 文件 | 用途 |
|---|---|
| `plan.json` | 一份真实的 5 任务 DAG（写作流水线），用 `mesh plan` 看批次、`mesh submit` 推进队列 |
| `nodes.json` | 控制面要接入的机器清单，喂给 `mesh hub --nodes-file` |

## 一条命令跑通（不用第二台机器）

```sh
python3 tools/demo.py
```

起一个节点、一个控制面，推 3 条任务，跑完打印产物。

## 两台机器（真实用法）

手机上（Termux + proot，或者任何 Linux）：

```sh
mesh node --id 手机 --root work=~/proj --token 一个长口令 --port 8765 --slots 1 --tags llm
```

常开的那台机器上：

```sh
mesh hub --db mesh.db --nodes-file examples/nodes.json --port 8788 --token 一个长口令
mesh submit examples/plan.json --db mesh.db
```

然后浏览器打开 `http://那台机器的IP:8788/?token=一个长口令`，作业一个个往下走。

计划里的 `req.tags` 会挑节点：`llm` 的任务只落在带 `llm` 标签的机器上，
`gpu` 的任务只落在带 `gpu` 的机器上。没有匹配的节点时作业会停在"等待"，
面板和 `mesh status` 会写出为什么等（缺标签、缺工具、槽位满、或者节点掉线）。
