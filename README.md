# Mesh —— 个人 AI 网格

[![CI](https://github.com/wangzi5151/mesh/actions/workflows/ci.yml/badge.svg)](https://github.com/wangzi5151/mesh/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue)](https://www.python.org/)
[![零第三方依赖](https://img.shields.io/badge/deps-0-brightgreen)](#开发)
[![License: MIT](https://img.shields.io/badge/license-MIT-green)](LICENSE)

> 你的手机、旧笔记本、树莓派上各装着一个不一样的 AI agent，它们互相不知道对方存在。
> 这个工具把它们连成一张网：**一个大任务拆成 DAG，谁空谁领，各自在自己机器的目录里干，
> 改动回来统一验收；手机浏览器打开一个网页就能看全程。**

```
              ┌─────────────────────────────────────────┐
              │  hub（任意一台常开的机器）                │
              │  心跳 · 派发 · 收尾 · 面板                │
              └───┬──────────────┬──────────────┬───────┘
                  │ 同一局域网     │              │
          ┌───────┴──────┐ ┌─────┴────────┐ ┌───┴─────────┐
          │ 手机          │ │ 旧笔记本      │ │ 树莓派       │
          │ Termux+proot │ │ 本地大模型     │ │ 只当文件仓库  │
          │ mesh node    │ │ mesh node    │ │ mesh node   │
          └──────────────┘ └──────────────┘ └─────────────┘
```

## 它解决的是什么问题

单个 agent 的天花板很低：一台手机跑一个模型，一个任务从头做到尾，一次只能干一件事。
而多数"大任务"其实是能并行的 —— 写篇文章可以同时查资料和做封面，改个仓库可以同时跑三处重构。

Mesh 只做三件事，做扎实：

| 能力 | 解决的问题 | 实现 |
|---|---|---|
| **统一寻址** | 五个 agent 有五种说"文件在哪"的方式 | `mesh://node/env:root/path`，一套语法全网通用 |
| **能力调度** | 你不知道哪台机器现在闲着、能不能跑这个任务 | 每台机器报 CPU/内存/工具/模型/标签，按任务需求打分挑节点 |
| **作业队列** | 手机睡着了、断电了、任务跑到一半崩了 | SQLite 持久队列 + 租约 + 崩溃恢复，重连接着干 |

一句话：**不训练模型、不替代 agent、不做云服务**。它只负责"谁在哪儿干什么、干完了没有"。

## 五分钟跑起来

只需要 Python 3.8 以上，不用 `pip install` 任何东西。

### 先看它能不能跑

```sh
python3 tools/demo.py
```

起一个节点 + 一个控制面，推 3 条任务的 DAG，两轮跑完：

```
节点   demo  http://127.0.0.1:39683
提交   3 条作业：t1 t2 t3

第 1 轮  完成 2  失败 0  退回 0  等待 0
第 2 轮  完成 1  失败 0  退回 0  等待 0

结果   done 3
  a.txt    '第一行\n第二行\n'
  b.txt    '自己一份\n'
```

`t1` 和 `t3` 同一批并行跑，`t2` 依赖 `t1`，自动等到第二批。

### 真的要两台机器

手机上（Termux + proot，或任何 Linux）—— 用自签证书就顺手把链路加密了：

```sh
mesh node --id 手机 --root work=~/proj --token 节点口令 --port 8765 --slots 1 --tags llm \
     --tls-cert cert.pem --tls-key key.pem
```

常开的那台机器上，把节点登记进库里（**重启不会再丢**），再发一把只能提交 `blog` 的令牌：

```sh
mesh nodes add 手机 https://手机IP:8765 --token 节点口令 --tags llm --db mesh.db
mesh token add --perms submit,read --project blog --label 博客 --db mesh.db
# → 只显示这一次的令牌，抄下来

mesh hub --db mesh.db --mirror ./产物 --port 8788 --token 管理口令 \
         --tls-cert cert.pem --tls-key key.pem --tls-ca cert.pem
```

手机浏览器打开 `https://那台机器的IP:8788/?token=管理口令` —— 节点在线情况、每条作业的
状态和产出，一屏看完。

用那把受限令牌提交作业（脚本里带上就行，不需要管理员口令）：

```sh
curl -k -X POST https://那台机器的IP:8788/rpc/submit \
     -H 'Authorization: Bearer 受限令牌' \
     -d '{"plan":{"project":"blog","tasks":[{"id":"a","prompt":"列提纲"}]}}'
```

没装进 PATH 也能用：`python3 -m mesh.cli node ...`。

## 模块分工

```
mesh/address.py     211 行  统一寻址：解析、规范化、通配匹配（支持中文名）
mesh/delta.py       521 行  块级增量：Adler-32 滚动校验 + sha256，rsync 式差分
mesh/capability.py  398 行  能力注册表：本机探测、调度打分、节点清单
mesh/queue.py       783 行  持久队列：WAL + 租约 + 调度权 + 节点表 + 令牌表
mesh/task.py        349 行  任务图：DAG 校验、批次、写作用域匹配
mesh/wire.py        424 行  零依赖 HTTP + JSON-RPC，可选 TLS
mesh/agent.py       678 行  节点 agent：路径围栏、作用域强制、越界回滚
mesh/hub.py         637 行  控制面：心跳、派发、重试、产物回收、面板
mesh/auth.py        167 行  令牌与权限：项目隔离、只读/可提交/可管理
mesh/cli.py         852 行  命令行：node / hub / submit / nodes / token / …
```

`--` 运行时零第三方依赖。整个包只用标准库，因为目标环境是手机上的 proot，
`pip install cryptography` 这种做法在那上面会先让你在编译环节绝望半小时。

## 地址语法

```
mesh://node-a/ubuntu:project/src/main.py
       └─ 节点 ─┘└环境┘└root┘└── 路径 ──┘
```

- `node` —— 节点名（`mesh node --id`）
- `env` —— optional，同一台机器上的不同环境（`ubuntu`、`termux`、`wsl`）
- `root` —— 命名目录，**必须**是节点上已经用 `--root name=path` 声明过的
- `path` —— root 内的相对路径，支持 `*` 通配

root 是这个设计的关键：agent 能碰的文件**只能在声明过的目录里**，一个作业能改什么
由计划里的 `write` glob 决定，越界就整批拒绝。

## 任务图

一份计划就是一个 JSON（见 `examples/plan.json`）：

```json
{
  "project": "blog",
  "tasks": [
    {"id": "outline", "prompt": "列提纲", "write": ["outline.md"], "priority": 10},
    {"id": "draft", "deps": ["outline"], "prompt": "写初稿", "write": ["draft.md"]},
    {"id": "cover", "req": {"tags": ["gpu"]}, "write": ["cover.png"]}
  ]
}
```

| 字段 | 作用 |
|---|---|
| `id` | 作业 id，一个项目内唯一 |
| `deps` | 上游作业 id 列表，全部 `done` 才解锁 |
| `prompt` | 给 agent 的提示词（通过环境变量 + stdin 传，不做字符串拼接） |
| `cmd` | 要执行的命令（节点需开 `--exec`），和 `prompt` 二选一或并存 |
| `read` / `write` | 读写作用域 glob。`write` 是硬约束，越界直接判失败 |
| `req` | 节点要求：`tags` / `min_cores` / `tools` / `agent` / `root` / `min_mem_mb` / `min_disk_mb` |
| `priority` | 越大越先派 |
| `max_attempts` | 这条作业最多试几次（0 或省略 = 用队列默认 2） |
| `model` / `verify` | 透传给 agent 的提示信息 |

`mesh plan plan.json` 会打印并行批次和**写冲突**：

```
$ mesh plan examples/plan.json
项目 blog：5 条任务，4 个并行批次
  第 1 批  cover outline
  第 2 批  draft
  第 3 批  facts
  第 4 批  publish
写作用域没有冲突
```

写冲突指的是"两条没有先后关系的任务写同一片区域"。这种计划**默认拒绝提交** ——
两个 agent 并行改同一段代码，结果是静默的数据损坏，宁可在提交时就挡下来。
确实要并行跑就 `--force`。

## 调度语义

`Hub.tick()` 一轮做四件事：

1. **回收** —— 租约过期的 `running` 作业回队列（那台机器掉线了，别人接着干）
2. **心跳** —— 向每个节点要一份能力快照，连不上的立刻移出调度池
3. **派发** —— 按优先级遍历就绪作业，挑节点，领取，发过去
4. **收尾** —— 成功 → `done`（解锁下游）；失败 → 按 `attempts` 决定重试还是 `failed`

三处刻意的设计：

**先挑节点、再领作业。** 反过来的话，队列会先把作业标成 `running`，然后发现没有节点
能跑它 —— 这条作业就占着租约空等，直到超时被回收。节点一多，整个队列会被这类作业堵死。

**租约必须比派发超时更长。** 否则一个在节点上正常跑着的长作业会被误判成掉线作业、
退回队列、再由别的节点重跑一遍：同一份改动执行两次。`Hub` 默认把租约设成
`dispatch_timeout + 60s`。

**一轮之内按节点记账。** 能力快照是上一轮心跳拍的，一个只有 1 个槽位的节点，
光看快照会在一轮里被塞进 10 条作业。`tick` 自己记着本轮给每个节点派了几条，
派满就换下一个节点，快照过期也不会超发。

失败分两类，别混：

- **派发失败**（节点连不上、JSON 都没送到）→ `unlease`，退回队列，作业根本没跑起来
- **业务失败**（跑起来了但报错、或者越界写）→ `fail`，错误原因写进 `feedback`，
  下一次 attempt 带着它重跑

### 多个控制面同时盯着

`mesh hub` 起两个（换机器、或者滚动重启）不会互相踩：调度权是库里的一把带租约的锁，
只有一个在派发，其余待命并在日志里说清楚"调度权在谁手上"。持有者每轮续租，进程被杀
或机器断电后租约自然过期，另一个接手 —— 不需要手工清锁。

```sh
# 机器 A
[02:31:02] 节点 2 在线/0 掉线
# 机器 B
[02:31:04] 待命：调度权在 hub-8123-4f2a1c9b 手上
```

### 产物回收：只传增量

`mesh hub --mirror DIR` 之后，作业改过的文件会被收回来，落在
`DIR/<节点>/<root>/<相对路径>`。已经有旧版本的文件走**块级差分**：把本地旧文件的块校验表
发给节点，节点只回"哪些块能复用 + 哪些是新字节"。

```
job     传输      文件大小   省
a       24000 B   24000 B    0.0%    full      ← 第一次只能整取
b        9519 B   24018 B   60.4%    delta     ← 之后只传变化的部分
```

两个细节值得说：

- **签名表由接收方算。** rsync 的方向就是这样：谁手上有旧版本，谁出签名表。反过来
  问节点要签名表是错的 —— 节点只有新版本，签名跟本地旧文件对不上，永远退化成整取。
- **不划算就整取。** 每块的强校验是 64 个字符的 sha256 hex（≈88 字节），小文件上签名表
  可能比文件本身还大。所以先算账：`上行签名 + 下行差分 >= 文件大小` 就直接整取。
  块大小也按"签名表最多 96 块"兜了个底，不然 rsync 的 sqrt 经验值在这里会翻车。

### 越界写会被回滚

作业跑之前，节点把 root 里的文件备份到临时目录；跑完比对快照，一旦发现越界改动
（改、新建、**或者删除**作用域外的文件），就把改动全部还原：改过的写回去、新建的删掉、
删掉的放回来、新建的空目录清掉，然后复核一遍"是不是真回到基线了"。

```json
{"violations": ["other/b.txt", "junk/out.txt", "victim.txt"],
 "rollback": {"restored": ["keep.txt", "victim.txt"], "removed": ["junk/out.txt"],
              "pruned_dirs": ["junk"], "clean": true}}
```

备份有上限（默认 20k 文件 / 64MB，`--backup-max-mb` 可调）。超出上限就不备份，
在结果里明说"这次只检测不撤销"，而不是假装自己防住了 —— 为了备份把机器拖死更糟。
不想要这个开销就 `mesh node --no-rollback`。

## 安全模型

一台开了 `--exec` 的节点等于一台远程可控的机器，所以默认是关的，开了也必须有令牌。

| 层 | 机制 |
|---|---|
| 传输 | `--tls-cert/--tls-key` 开 HTTPS（TLS 1.2+）；客户侧 `--tls-ca` 校验证书、`--tls-insecure` 兼容自签 |
| 身份 | 所有 `/rpc/*` 都要 `Authorization: Bearer <token>`；`/health` 例外，方便探活 |
| 隔离 | 令牌绑项目：`blog` 的令牌提交不了 `code` 的作业（见下） |
| 路径 | 每个 root 用 `realpath` 解析后验证包含关系 —— **符号链接逃逸也会被拦下** |
| 写权限 | `--root name=path:ro` 的 root 拒绝一切写操作 |
| 作用域 | 作业前后各走一遍 root 建快照，改/建/**删**逐个比对 `write` glob，越界就回滚改动并把整条作业判失败 |
| 回收 | 节点报上来的产物路径逐个核对，不许带 `..`、不许绝对路径 —— 恶意节点也写不出镜像目录 |
| 执行 | `--exec` 不带 `--token` 直接拒绝启动 |

作用域校验是**事后**判定的：作业跑完、比对快照才知道越没越界。所以节点会**先把文件备份
一份**，越界了再还原（见上一节的"回滚"）。备份超出上限时退化成"只检测不撤销"，
这一点一定会写进结果里，不会假装成功。

### 多租户：一把钥匙只开一扇门

```sh
mesh token add --perms submit,read --project blog --label 博客   # 只能提交 blog
mesh token add --perms read --project ""        --label 看板的   # 只读，所有项目
mesh token add --perms admin --project blog     --label 博客管理员
mesh token ls
```

权限四档：`read` / `submit` / `cancel` / `admin`（`admin` 蕴含前三档）。
项目名留空表示所有项目。库里只存 sha256，明文只在创建的那一秒出现过，文件权限 `0600`。

没有配置任何令牌时控制面是**开发模式**（谁都能连），`mesh hub` 启动时会直接提示你
"对同网段完全开放" —— 局域网自用没问题，但别在咖啡厅的 WiFi 上这么跑。

### 节点清单会记住

`mesh nodes add` 写进库里，控制面重启自动恢复，不用每次敲一长串 `--node`：

```sh
mesh nodes add 手机 https://192.168.1.20:8765 --token 节点口令 --tags llm --weight 1
mesh nodes add 笔记本 https://192.168.1.31:8765 --token 节点口令 --tags gpu,fast --weight 2
mesh nodes ls
mesh nodes rm 手机
```

比对用的是**作业前后两次快照做差**（`mtime_ns` + 大小），不是"记下开始时间、
事后找 mtime >= 开始时间"。后者看着更省事，但文件系统的 mtime 和 `time.time()`
不是同一个精度、甚至会小幅偏移，同一瞬间写出的文件会被判成"没改过" ——
写作用域校验是闸门，漏检等于没有。

## 命令行

| 命令 | 干什么 |
|---|---|
| `mesh node --id 手机 --root work=~/proj` | 起节点，把本机目录和能力暴露出去 |
| `mesh hub --db mesh.db --mirror ./产物` | 起控制面，开始调度并回收产物 |
| `mesh submit plan.json --db mesh.db` | 推一份任务图进队列 |
| `mesh nodes add/ls/rm` | 管节点清单（存库里，重启不丢） |
| `mesh token add/ls/rm` | 管访问令牌（只存哈希，可绑项目） |
| `mesh plan plan.json` | 校验计划，预览批次和写冲突 |
| `mesh status --hub http://…` | 看节点在线情况和作业进度 |
| `mesh jobs --db mesh.db --show <id>` | 列作业 / 看某条作业的全文 |
| `mesh retry <id> --reset` | 把失败/阻塞的作业放回队列 |
| `mesh cancel <id>` | 取消作业 |
| `mesh doctor --root work=.` | 体检：本机能不能当节点、目录和队列能不能写 |

设计上只有一条原则：**队列文件就是接口**。`hub` 跑在一个进程里，`submit` 和 `status`
在另一个进程、甚至另一台机器（挂同一个目录）上操作同一份 SQLite 队列，不需要额外服务，
也不需要账号。所有命令都支持 `--db` 指定队列位置。

`mesh hub --once` 只跑一轮并把结果打成 JSON，调 cron 或者调试时好用。

## HTTP 接口

控制面（`mesh hub --port 8788`）和节点都是同一套协议：

```sh
# 脚本用：POST /rpc/<方法>
curl -s -X POST http://host:8788/rpc/status -H 'Authorization: Bearer 口令'
curl -s -X POST http://host:8788/rpc/jobs -d '{"state":"failed"}'
curl -s -X POST http://host:8788/rpc/submit -d '{"plan":{"project":"p","tasks":[{"id":"a"}]}}'

# 人看：GET /（面板）和 GET /status（JSON）
# 浏览器发不了自定义头，所以 GET 允许 ?token=
open 'http://host:8788/?token=口令'
```

节点的方法就是文件操作那套：`cap` `ls` `stat` `read` `write` `sig` `delta` `patch`
`exec` `job`。返回统一是 `{"ok": true, "r": ...}` 或 `{"ok": false, "e": "人话", "t": "错误种类"}`。

控制面多三个方法：`status`、`jobs`、`submit`（外加只读的 `ping` 和两个 GET 路径）。
**每个方法都按令牌的项目和权限判一次** —— 拿 `blog` 的令牌 submit `code` 的作业会被
`401 令牌只能用于项目 blog` 挡下来，而不是悄悄建进去。

开了 TLS 之后全部走 `https://`；`curl` 对自签证书记得加 `-k`。

## 已知限制

老实列出来，免得你以为有：

- **节点口令在库里是明文。** 控制面得拿它去连节点，没法只存哈希；能做的都做了 ——
  队列文件 `0600`、令牌（对外发的）只存 sha256。真怕就把 `mesh.db` 放加密卷里。
- **没有账号体系。** 令牌就是钥匙，没有用户、没有过期时间、没有审计日志。
  "个人网格"的取舍，不是没想过。
- **单机单写者以外的一致性靠 SQLite。** 调度权锁能保证只有一个 `hub` 在派发，
  但把 `mesh.db` 放在网络文件系统（NFS/SMB）上，SQLite 的锁不保证可靠 —— 别这么干。
- **回滚有上限。** 默认 20k 文件 / 64MB，超了只检测不撤销（并在结果里说明）。
  大仓库请 `--no-rollback` 或者把 `write` 收到最小。
- **删除靠"基线里少了"来发现。** 作业跑之前不存在、跑完也不存在的文件，两边都看不见。
- **同一秒内、同大小、同路径的改写可能漏判**（快照比对是 `mtime_ns` + 大小）。
  要更严就换成内容哈希，代价是每次作业多扫一遍所有文件。
- **产物回收是单向的。** 从节点收回控制面，不会自动推给别的节点。
  "A 机器改完要同步到 B 机器"这一步还没做 —— 现在得靠作业自己 `read` 上游产物。
- **没有限流。** 令牌对了就能一直提交，`MAX_BODY`（64MB）是唯一的闸。

## 开发

## 开发

```sh
make test     # python3 -m unittest discover -s tests -v
make lint     # 零第三方依赖检查（CI 硬门槛）
make check    # 编译 + 零依赖 + 全部测试
make demo     # 端到端演示
```

471 个测试，全部走真实路径：调度器的测试会真的起 HTTP 服务和节点、真的把作业派出去、
真的写文件。因为这一层的价值就在"跨机器跑完再收回来"，把网络换掉的 mock 测不到故障路径 ——
而这套代码里最容易错的就是故障路径（节点掉线、租约过期、越界写、重复执行）。

引入第三方依赖会让 `make lint` 失败，这是有意的。

```
tests/test_address.py     寻址解析（含中文节点名）
tests/test_delta.py       块级差分
tests/test_capability.py  能力探测与打分
tests/test_queue.py       队列、租约、调度权锁、节点表、令牌表
tests/test_task.py        DAG 与作用域
tests/test_wire.py        HTTP/RPC、TLS、权限钩子
tests/test_auth.py        令牌与项目隔离
tests/test_agent.py       节点：围栏、差分、删除检测、越界回滚
tests/test_hub.py         控制面：派发、重试、回收、持久化、调度权、产物增量、多租户
tests/test_cli.py         命令行
```

TLS 的用例需要 `openssl` 生成自签证书，机器上没有就自动跳过。

协议版本是 `PROTO = 1`，`mesh://` 地址和 JSON-RPC 的形状变了就要 +1。

## License

MIT
