# 示例

| 文件 | 用途 |
|---|---|
| `plan.json` | 一份真实的 5 任务 DAG（写作流水线），用 `mesh plan` 看批次、`mesh submit` 推进队列 |
| `nodes.json` | 一次性导入的节点清单（喂给 `mesh hub --nodes-file`，之后用 `mesh nodes` 维护） |

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
# 1. 把节点记进库里（重启不丢），顺便发一把只能提交 blog 的令牌
mesh nodes add 手机 https://手机IP:8765 --token 节点口令 --tags llm --db mesh.db
mesh token add --perms submit,read --project blog --label 博客 --db mesh.db

# 2. 起控制面：产物流向 ./产物，面板 8788
mesh hub --db mesh.db --mirror ./产物 --port 8788 --token 管理口令

# 3. 推计划
mesh submit examples/plan.json --db mesh.db
mesh status --hub http://127.0.0.1:8788 --hub-token 管理口令
```

然后浏览器打开 `http://那台机器的IP:8788/?token=管理口令`，作业一个个往下走。

镜像目录长这样 —— 每个节点、每个 root 各有自己的子目录，收到的文件跟节点上一模一样：

```
产物/
  手机/
    work/
      outline.md
      draft.md
```

第二次改同一个文件只传增量：`artifacts.ratio` 会告诉你省了多少。

计划里的 `req.tags` 会挑节点：`llm` 的任务只落在带 `llm` 标签的机器上，
`gpu` 的任务只落在带 `gpu` 的机器上。没有匹配的节点时作业会停在"等待"，
面板和 `mesh status` 会写出为什么等（缺标签、缺工具、槽位满、或者节点掉线）。
