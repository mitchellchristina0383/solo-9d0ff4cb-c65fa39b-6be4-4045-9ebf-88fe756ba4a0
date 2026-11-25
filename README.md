# 分片告警合并查询服务

值班员排障时从三个告警分片查询最近 N 条告警的 FastAPI 服务。三个分片各持一份
**只追加**的 SQLite 库；查询只看查询开始时已提交的数据，按
`(timestamp, id)` 倒序跨分片合并，并在给定查询时限内尽早返回。

## 1. 需求与实现对照

| 需求 | 实现 |
| --- | --- |
| FastAPI 接收 N 和查询时限 | `GET /alerts/latest?n=&timeout=`，参数经 Pydantic 校验（n≥1，timeout>0） |
| 三个分片各一份 SQLite、只追加 | `app/shard.py`，每分片一个 `shard_{i}.db`；写入只有 `INSERT`，重复编号返回 409 |
| 只看查询开始时已提交的告警 | 每个查询用独立连接开启只读事务并立即发一次 SELECT，把 WAL 读快照钉在开始时刻；查询中的补报对本查询不可见 |
| 按时间和唯一编号倒序合并 | 排序键 `(timestamp DESC, id DESC)`，同时间戳由 id 决胜 |
| 分片先给最新排序键，再给明细 | 合并第一阶段并行 `head()` 取各分片最新 key；第二阶段才按需读明细 |
| 每批最多读 20 条 | `ALERT_BATCH_SIZE=20`，`read_batch` 以行值游标 `(timestamp,id) < cursor` 翻批 |
| 确认前 N 条立即返回 | 惰性 k 路归并：只有“全局最大上界”属主缓冲为空时才读批；确认够 N 条立刻返回，较旧分片明细不读 |
| 读完仍不足 N 返回全部 | 全部分片耗尽返回 `status=exhausted` 和全部可见告警 |
| 到时补不齐返回最长有序前缀并指出阻塞分片 | `status=timeout`、`complete=false`，`blockers` 给出分片号、阶段（head/batch）、原因、耗时 |
| **分片巡检：点名分片 + 总时限，录入→新快照分批读回同一条** | `POST /probe/{shard_id}?timeout=`（/docs「分片巡检」分组），走原 `ShardRegistry.append → Shard.append` 链路写 `is_probe=1` 探测行，再开新快照用同款 `read_batch(include_probes=True)` 翻批寻找；只有读回才 `passed=true` |
| **失败指明卡在录入还是读回** | 响应 `stage`/`stage_label` 为 `append/录入` 或 `readback/读回`，并分别给出 `append` / `readback` 明细与错误 |
| **到时先返回，超时后才提交也清理** | HTTP 只在总时限内等结论，超时返回 `status=timeout`；后台任务不取消，写锁阻塞导致录入在响应后才提交时，仍在 `finally` 删除探测行；`GET /probe/jobs/{job_id}` 轮询最终结果 |
| **重启清掉中断遗留探测行** | lifespan 启动先跑 `ProbeManager.cleanup_on_startup()`，`DELETE … WHERE is_probe=1`；`/health.probe_startup_cleanup` 报告各分片删除数 |
| **正式查询始终排除探测行** | `head` / `count_visible` / `read_batch` 默认全部带 `WHERE is_probe=0`：可见条数、最新排序键、明细都不算探测行；旧库启动自动 `ALTER TABLE ADD COLUMN is_probe … DEFAULT 0` |
| Docker Compose 启动，/docs 录入查询 | `Dockerfile` + `docker-compose.yml`；启动后访问 `http://localhost:8000/docs` |
| 样例可给明细读取加延迟 | `ALERT_SHARD_DELAYS="0,0,0.4"`，延迟在 asyncio 层、可被时限即时取消（只加在明细批次，head 不延迟） |

### 合并算法要点（`app/merge.py`）

每个分片维护一个“剩余数据严格上界” `upper`：

- `head` 返回的 key 是初始上界；
- 读到一批后，上界是缓冲首条 key；缓冲耗尽但可能还有数据时，上界退化为上一批
  最后一条 key。

每轮取所有活动分片中上界最大的属主：

1. 属主缓冲有数据 → 其首条已被其它分片的上界夹住，**可确认输出**，不需要读任何
   较旧分片；
2. 属主缓冲为空 → 必须先读它的下一批，此时其它分片的行一条都不能提前输出
   （属主下一批可能存在更靠前的行）；
3. 读批前/等待中检查时限，超时即返回已确认的最长有序前缀，并把该属主记入
   `blockers`。

### 快照隔离的关键坑

Python 3.11 的 `sqlite3` 遗留隔离模式下，普通 `SELECT` 运行在 autocommit 中
（`in_transaction=False`），每次读都取最新快照，单独 `BEGIN` 而不立刻读也不会
固定视图。因此 `ShardSnapshot` 必须 **先 `BEGIN`，紧接着执行一次 `SELECT`**，
WAL 读快照才会被钉住（已用最小复现和测试验证）。

## 2. 目录结构

```
app/
  config.py     # 环境变量配置（分片数、库目录、延迟、批大小、默认时限）
  shard.py      # SQLite 只追加写入 + 只读快照（head / read_batch）；is_probe 标记与清理
  merge.py      # 惰性 k 路归并、时限控制、阻塞分片识别
  probe.py      # 分片巡检后台任务：录入→新快照分批读回→超时先返回→后台清理→启动清扫
  models.py     # 请求/响应模型（含 ProbeResponse）
  main.py       # FastAPI 路由与生命周期（含 /probe、/probe/jobs）
scripts/
  seed.py       # 幂等样例数据（68 条，含同时间戳决胜与慢分片专属新告警）
tests/          # 28 个 pytest：合并、分页、快照隔离、超时、API、补报、巡检与重启清理
Dockerfile
docker-compose.yml
```

## 3. 启动方式

### Docker Compose（交付主路径）

```bash
docker compose up --build
# 浏览器打开 http://localhost:8000/docs
```

compose 中分片 2 每批明细读取延迟 0.4s，用于演示超时。数据保存在命名卷
`alert-data`；清空数据：`docker compose down -v`。

### 本地直接运行（无 Docker 环境时的等价方式）

```bash
pip install -r requirements.txt
export ALERT_DB_DIR=./data ALERT_SHARD_DELAYS="0,0,0.4"
python scripts/seed.py
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 4. /docs 操作说明

1. `POST /alerts/{shard_id}`：手工指定 id 录入；`POST /alerts/{shard_id}/auto`
   自动生成 UUID，适合在 /docs 里做补报实验。
2. `GET /alerts/latest`：填 `n`（条数）与 `timeout`（秒）。
3. 响应字段：
   - `alerts` / `count`：已确认的有序结果（超时时为最长有序前缀）；
   - `status`：`complete`（拿满 N）/ `exhausted`（全库不足 N）/ `timeout`；
   - `shards[].head_key`：各分片本次可见的最新排序键；
     `shards[].batches_read`：实际读取的明细批次数（head 不计）；
   - `blockers`：超时场景下挡住后续的分片与阶段。
4. **分片巡检（本次新增）**：/docs 的「分片巡检」分组打开
   `POST /probe/{shard_id}`，填 `shard_id`（点名分片）和必填的 `timeout`
   （总时限秒数，也可用 JSON body `{"timeout": …}`），Execute：
   - `status=passed, passed=true` 才表示“录入 + 新快照分批读回同一条”全部成功，
     响应通常已带 `cleanup.deleted_rows=1`；
   - `status=timeout` 时看 `stage`：`append`=卡在录入（典型为分片写锁被占），
     `readback`=已录入但分批读回在总时限内没完成；`stage_label` 给中文环节名；
   - 请求到时先返回，后台仍会等迟到的录入提交并删除探测行；用响应里的
     `status_url`（`GET /probe/jobs/{job_id}`）轮询最终 `append` / `readback` /
     `cleanup` 结果与 `finished`。
   - 兼容别名（默认不在 /docs 展示）：`GET/POST /shards/{id}/probe`、
     `GET/POST /alerts/{id}/probe`、`GET/POST /probe?shard_id=`。
   - 探测行用 `is_probe=1` 标记、id 带 `probe-` 前缀；正式查询在 SQL 层排除。

## 5. 自测记录

### 自动化测试（28/28 通过，连续 3 轮稳定）

```
$ PYTHONPATH=.pylibs/lib/python3.11/site-packages python3 -m pytest tests/ -q
28 passed, 1 warning in 9.79s
```

原 15 个测试全部保持通过；新增 13 个巡检测试：

- 巡检 happy path：passed、读回批次≥1、`cleanup.deleted_rows=1`、库内无探测行；
- 路径/方法/body 兼容别名（`/alerts/{id}/probe`、`GET /probe/{id}`、
  `/probe?shard_id=`、JSON `{"timeout":…}`）；404 分片、422 缺总时限；
- 正式快照的 `count_visible` / `head` / `read_batch` 排除探测行，
  `include_probes=True` 仍可见；正式 `/alerts/latest` 的明细、`visible_rows`、
  `head_key` 均不含探测行；
- 探测行落在 25 条更新数据之后时，`include_probes` 分批翻 ≥2 批仍能读回；
- 读回超时：`stage=readback`、录入成功、后台清理删除 1 行；
- 进程内 asyncio 写锁阻塞：持锁期间总时限到，返回 append 超时且探测行未提交；
  放锁后“超时后才提交”，任务最终 `append_ok=true` 且清理 1 行；
- 真实 SQLite 跨连接 `BEGIN IMMEDIATE` 写事务阻塞录入（端到端 HTTP）；
- 启动清理删除三分片遗留探测行、保留正式行（服务级 + 完整 lifespan 两组）。

> 联调中发现并修复一个真实竞态：读回延迟最初写成
> `wait_for(sleep(delay), timeout=min(delay, remaining))`，预算充足时
> `timeout` 恰好等于 `delay`，`wait_for` 与 `sleep` 同时到期会抛
> `TimeoutError`，使“慢但健康”的分片在一批延迟后被误判读回超时（全量测试
> 按文件序跑到带 0.15s 延迟的分片 2 时复现）。改为与 `merge.py` 一致地用
> “剩余总预算”作为 `wait_for` 超时后，慢分片 happy path 与紧时限超时均正确。

### 巡检三项规定动作的真实 HTTP 实测（uvicorn，环境变量同 compose：`0,0,0.4`）

**① 写锁阻塞（跨进程 SQLite 写事务，分片 0，持有 4s，总时限 1s）**

```
[holder] BEGIN IMMEDIATE ... hold 4.0s
[client] POST /probe returned after 1.01s (budget 1.0s)
  status=timeout  stage=append(录入)  phase=append  in_progress=true
  append.ok(响应时)=null        # 录入仍堵在 SQLite 写锁上
[holder] ROLLBACK / write lock released
[job] final status=timeout stage=append finished=true caller_timed_out=true
      append_ok=true append_elapsed_ms=4035.0      # 超时响应之后才拿到锁并提交
      cleanup_done=true cleanup_deleted_rows=1
[db] probe_rows=0 formal_rows=0
DRILL_RESULT: PASS
```

**② 读回超时（分片 2 每批延迟 0.4s，总时限 0.1s）**

```
[client] POST returned after 0.11s
  status=timeout stage=readback(读回) append.ok=true elapsed_ms=100
  detail=总时限 0.1s 到时，巡检仍卡在【读回】阶段 ...
[job] final finished=true readback_ok=false
      error=到达总时限，第 1 批明细仍在延迟/读取
      cleanup_deleted_rows=1
DRILL_RESULT: PASS
```

对照：同一慢分片 2 把总时限放宽到 5s 时 `status=passed`，耗时 406ms
（恰好一批延迟），读回 1 批、清理删除 1 行——证明上面的超时是预算生效而非误判。

**③ 中断后重启（kill -9 + 遗留行）**

```
[kill] observed committed probe row probe-3e54...; SIGKILL pid 872
[kill] server process is dead
[kill] after SIGKILL probe_rows=1, killed probe row still on disk=True
```

另向分片 0/1 各插 1 行 `is_probe=1` 模拟任意中断遗留；此时三分片各 1 条探测行。
用同一 `/data` 目录重新启动：

```
GET /health -> {"probe_startup_cleanup":{"0":1,"1":1,"2":1}, ...}
startup removed leftover probe rows: {0: 1, 1: 1, 2: 1}
重启后库内 ALL PROBE ROWS: 0     # 正式行原样保留
重启后 POST /probe/1?timeout=5 -> passed=True, cleanup=1
```

**正式查询排除探测行的实测**：向分片 1 物理插入最新探测行
`ts=4200000001 / probe-exclusion-check`，再录一条较旧正式行
`ts=4100000000 / normal-exclusion-check`：

```
GET /alerts/latest -> 明细不含 probe-exclusion-check，含 normal-exclusion-check
shard 1 stat: visible_rows=1
              head_key=[4100000000.0, "normal-exclusion-check"]
库原始计数: shard 1 formal=1 probe=1 total_raw=2   # 行确实在，只是正式查询不可见
```

**旧库迁移实测**：手工建 v1 五列旧表 + 2 条历史行，`Shard(0)` 初始化后
自动补出 `is_probe` 列（默认 0），`count_visible=2`、`head=(1001,'old-2')`、
明细分页正常，`delete_all_probes()=0`。`/docs` 返回 200，OpenAPI 中
`/probe/{shard_id}` 的 `shard_id` 必填、`timeout` 参数可见。

### 原有真实 HTTP 实测（uvicorn + 种子数据，分片 2 延迟 0.4s/批）

- `n=5,timeout=5`：5 条全部来自分片 2，分片 0/1 `batches_read=0`，
  分片 2 仅 1 批，耗时约 0.4s（一批延迟）。
- `n=10`：跨 3 分片合并，同时间戳 `1000059.0` 上 `alert-0060` 正确排在
  `alert-0059` 前。
- `n=68,timeout=0.1`：`status=timeout,count=0`，`blockers=[{shard:2,phase:batch}]`，
  耗时恰好 100ms。
- 分片 2 扩到 56 条后 `n=55,timeout=0.5`：返回 20 条有序前缀，
  `blockers=[(2,batch)]`，旧分片 0 批次；放宽到 `timeout=1.0` 返回 48 条前缀
  （分片 2 两批 40 条 + 旧分片 8 条），第三批在途到点仍由分片 2 阻塞。
- 查询进行中向分片 1 补报一条 `timestamp=9999999` 的告警：在途查询
  `exhausted` 返回 98 条且不含该补报；随后新查询中该补报排第 1 条。
- 参数非法返回 422；重复 id 写入返回 409；`/docs` 返回 200。

## 6. 未完成项 / 限制

1. **本环境没有 Docker 守护进程**（`docker: command not found`、无
   `/var/run/docker.sock`），`docker compose up --build` 未在本机实跑；
   已用 PyYAML 校验 `docker-compose.yml` 语法，并按 compose 中的环境变量
   （`ALERT_SHARD_DELAYS=0,0,0.4`、`ALERT_BATCH_SIZE=20`、独立 `ALERT_DB_DIR`）
   用 `.pylibs` 里的依赖直接起 uvicorn 完成全部实测。`Dockerfile` 用
   `COPY app ./app` 整目录拷贝，新增的 `app/probe.py` 会自动进镜像；
   compose 无需改动。仍需在有 Docker 的环境真实 `up --build` 一次。
2. 阻塞分片目前报告“全局最大上界属主”这一直接阻塞者；若需要把所有在途未返回
   的分片都列出，可在后续版本扩展（协议上属主才是有序前缀的硬阻塞）。
3. 人工延迟是演示用的同步等待模拟，真实分片应替换为网络/远程 DB 客户端；
   合并引擎与分片接口未耦合，替换 `ShardSnapshot` 实现即可。
4. 未做鉴权与多租户隔离，按内部运维工具假设部署在可信网络。
5. 测试使用 FastAPI `TestClient`（基于 httpx 的同步封装，有一条 starlette
   弃用告警，不影响功能）。
6. 巡检任务表 `ProbeManager.jobs` 是**进程内内存态**，与 compose 的单 uvicorn
   worker 对应；多 worker / 多副本部署时 `GET /probe/jobs/{id}` 可能落到没有该
   任务的进程。核心安全性不依赖该表：探测行有 `is_probe=1` 标记，任何进程重启
   都会在启动时清扫，后台清理也在录入所在进程内完成。后续若水平扩展，可把任务
   状态落到 SQLite/Redis，并给 jobs 表加 TTL/条数上限（当前不主动淘汰）。
7. SQLite 写等待受连接 `busy_timeout=30s` 约束：若第三方写事务持锁超过 30s，
   探测录入会收到 `OperationalError` 并判为 `failed/stage=append`；残留行仍由
   清理重试与下次启动清扫兜底。直接用 SQL 排查库时，正式数据请带
   `WHERE is_probe=0`（或依赖启动清扫后的干净状态）。
