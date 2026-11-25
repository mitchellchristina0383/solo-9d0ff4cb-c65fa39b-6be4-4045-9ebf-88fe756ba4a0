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
  shard.py      # SQLite 只追加写入 + 只读快照（head / read_batch）
  merge.py      # 惰性 k 路归并、时限控制、阻塞分片识别
  models.py     # 请求/响应模型
  main.py       # FastAPI 路由与生命周期
scripts/
  seed.py       # 幂等样例数据（68 条，含同时间戳决胜与慢分片专属新告警）
tests/          # 15 个 pytest：合并、分页、快照隔离、超时、API、查询中补报
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

## 5. 自测记录

### 自动化测试（15/15 通过）

```
$ pytest tests/ -q
15 passed in 7.99s
```

覆盖：基本合并与同时间戳 id 决胜、前 N 条早退且旧分片 0 批次、20 条分页、
跨分页交错归并、空分片、head/batch 两阶段超时、部分有序前缀 + 阻塞分片、
慢但未超时成功、SQLite 快照隔离、查询中补报不可见、API 参数校验与 409 冲突。

### 真实 HTTP 实测（uvicorn + 种子数据，分片 2 延迟 0.4s/批）

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

1. **本环境没有 Docker 守护进程**，`docker compose up --build` 未在本机实跑；
   Dockerfile/compose 已按标准 Python 3.11-slim 镜像编写，需在有 Docker 的
   环境验证一次。
2. 阻塞分片目前报告“全局最大上界属主”这一直接阻塞者；若需要把所有在途未返回
   的分片都列出，可在后续版本扩展（协议上属主才是有序前缀的硬阻塞）。
3. 人工延迟是演示用的同步等待模拟，真实分片应替换为网络/远程 DB 客户端；
   合并引擎与分片接口未耦合，替换 `ShardSnapshot` 实现即可。
4. 未做鉴权与多租户隔离，按内部运维工具假设部署在可信网络。
5. 测试使用 FastAPI `TestClient`（基于 httpx 的同步封装，有一条 starlette
   弃用告警，不影响功能）。
