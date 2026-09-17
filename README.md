# 可靠 Webhook 投递服务

基于 **Python 3.12 + FastAPI + PostgreSQL** 的可靠 Webhook 投递系统，使用
**Docker Compose** 一键启动。

能力：

- **事件幂等提交**：生产者以 `(source, event_id)` 提交事件；重复提交返回原事件
  （HTTP 200，响应体 `duplicate: true`），**不新增任何投递**。
- **严格顺序投递**：同一订阅（source → target）严格按事件顺序处理——前一事件
  成功或进入死信之前，后一事件不会被发送。
- **租约抢占与接管**：Worker 从数据库认领任务并持有租约；Worker 宕机/卡死导致
  租约过期后，其他 Worker 可接管。回写带租约令牌栅栏，过期结果会被丢弃
  （at-least-once 语义，极端接管场景下目标端可能收到一次重复投递）。
- **HMAC 签名**：每次投递携带时间戳签名头
  `X-Webhook-Signature: t=<unix秒>,v1=<HMAC-SHA256 hex>`，签名内容为
  `<timestamp>.<紧凑排序 JSON 载荷>`。
- **指数退避 + 死信**：失败按 `base * 2^(n-1)` 退避（封顶），**最多 6 次尝试**
  后进入死信队列。
- **投递历史 / 死信重放**：可查询投递与每次尝试明细；重放会在同一投递链
  （`chain_id`）上**新建投递记录**（`chain_seq + 1`），旧记录完整保留。
- **本地回调接收器**：内置带签名校验、成功/失败/前 N 次失败/慢响应等端点的
  Receiver，便于本地联调与演示。

## 架构

| 服务 | 端口（宿主机） | 说明 |
| --- | --- | --- |
| `db` | 5432 | PostgreSQL 16，数据卷 `pgdata` 持久化 |
| `api` | 8000 | FastAPI：提交事件、订阅管理、历史/死信/重放 API |
| `worker` | — | 投递进程，靠数据库行锁认领任务，可水平扩展 |
| `receiver` | 8001 | 本地回调接收器（开发/联调用） |

投递顺序与租约在数据库层实现（核心见 `app/repository.py`）：

1. 每个订阅的投递有一个全局单调的 `seq`；Worker 只认领该订阅 **seq 最小且未终结
   且前面不存在 pending/in_flight 投递** 的任务（`FOR UPDATE SKIP LOCKED`），
   多 Worker 并发不会重复认领。
2. 认领后状态为 `in_flight`，设置 `leased_by`（租约令牌）与
   `lease_expires_at = now() + LEASE_SECONDS`。
3. 投递完成后，记录尝试与结算都以租约令牌为条件；租约已被接管时本次结果作废。
4. 成功 → `succeeded`；失败且未满 6 次 → 回到 `pending` 并设置指数退避时间
   `not_before`；满 6 次 → `dead_lettered`，其后同订阅的下一事件才允许发送。

> 生产配置要求 `LEASE_SECONDS`（默认 30）大于 `HTTP_TIMEOUT_SECONDS`（默认 10），
> 因此存活 Worker 不会在请求飞行中失去租约；接管只针对真正僵死的 Worker。

## 快速开始

```bash
docker compose up --build
```

启动后：

- API 文档（Swagger UI）：<http://localhost:8000/docs>
- API OpenAPI：<http://localhost:8000/openapi.json>
- API 健康检查：<http://localhost:8000/health>
- 回调接收器：<http://localhost:8001/> （查看记录：<http://localhost:8001/received>）

扩容 Worker（严格顺序仍然成立，只是不同订阅可并行）：

```bash
docker compose up --scale worker=3
```

### 30 秒体验流程

```bash
# 1) 注册订阅：source=orders 的事件投递到本地接收器
curl -s -X POST localhost:8000/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"source":"orders",
       "target_url":"http://receiver:8001/receive",
       "secret":"dev-signing-secret"}'

# 2) 提交事件（首次 201）
curl -i -X POST localhost:8000/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"orders","event_id":"evt-1001","payload":{"order":42}}'

# 3) 再次提交同一事件 -> 200，duplicate=true，不新增投递
curl -i -X POST localhost:8000/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"orders","event_id":"evt-1001","payload":{"order":42}}'

# 4) 查看投递历史与每次尝试明细
curl -s 'localhost:8000/api/v1/deliveries?source=orders' | python3 -m json.tool
#   取返回中的 id：
curl -s localhost:8000/api/v1/deliveries/1/attempts | python3 -m json.tool

# 5) 查看接收器实际收到的内容（含签名校验结果）
curl -s localhost:8001/received | python3 -m json.tool
```

### 失败退避与死信重放

```bash
# /fail 始终返回 500
curl -s -X POST localhost:8000/api/v1/subscriptions \
  -H 'Content-Type: application/json' \
  -d '{"source":"billing",
       "target_url":"http://receiver:8001/fail",
       "secret":"dev-signing-secret"}'

curl -s -X POST localhost:8000/api/v1/events \
  -H 'Content-Type: application/json' \
  -d '{"source":"billing","event_id":"bill-1","payload":{}}'

# 等待 6 次尝试（默认退避 5,10,20,40,80 秒，约 2.5 分钟）
curl -s 'localhost:8000/api/v1/dead-letters?source=billing' | python3 -m json.tool

# 让接收器不再失败，然后重放（返回 201，新投递属于同一 chain_id，chain_seq=2）
curl -s -X POST 'localhost:8001/control/fail?enabled=false'
curl -i -X POST localhost:8000/api/v1/dead-letters/<delivery_id>/replay
```

也可把目标设为 `http://receiver:8001/fail/2`：对同一投递前 2 次返回 500、
第 3 次返回 200，可快速观察退避后重试成功。

## API 一览

所有业务 API 前缀为 `/api/v1`，交互文档见 `/docs`。

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/events` | 提交/幂等提交事件；首次 201，重复 200 |
| GET | `/events/{id}` | 查询事件 |
| GET | `/events/{id}/deliveries` | 事件对应的所有投递 |
| POST | `/subscriptions` | 注册订阅（同 source+URL 重复注册为重新激活） |
| GET | `/subscriptions?source=` | 列出订阅 |
| GET | `/subscriptions/{id}` | 查询订阅 |
| GET | `/deliveries?status=&subscription_id=&source=` | 投递历史 |
| GET | `/deliveries/{id}` | 投递详情 |
| GET | `/deliveries/{id}/attempts` | 每次尝试明细（状态码、响应摘要、错误、租约时间） |
| GET | `/dead-letters?subscription_id=&source=` | 死信列表 |
| POST | `/dead-letters/{id}/replay` | 死信重放（201；非死信 409；链上有未完成投递 409） |
| GET | `/health` | 健康检查 |

投递状态：`pending`（等待/退避中）、`in_flight`（已被 Worker 认领）、
`succeeded`、`dead_lettered`。

### 投递请求格式（Worker → 你的回调）

```
POST <target_url> HTTP/1.1
Content-Type: application/json
X-Webhook-Signature: t=1758000000,v1=ab12...
X-Webhook-Event: orders
X-Webhook-Event-Id: evt-1001
X-Webhook-Delivery-Id: 7
X-Webhook-Attempt: 1
```

- 载荷为紧凑 JSON（键排序、无空白），签名原文 `"<t>.<载荷>"`，HMAC-SHA256
  使用订阅密钥。只有响应 **2xx** 视为成功；连接错误、超时、非 2xx 均计为一次失败。
- 各语言验签示例（Python）：

```python
import hmac, hashlib, json
expected = hmac.new(
    secret.encode(),
    f"{timestamp}.{json.dumps(body, sort_keys=True, separators=(',', ':'))}".encode(),
    hashlib.sha256,
).hexdigest()
hmac.compare_digest(expected, signature_v1)
```

## 本地回调接收器

默认随 Compose 启动（容器内 8001，宿主机 `http://localhost:8001`）：

| 方法 | 路径 | 行为 |
| --- | --- | --- |
| POST | `/receive` | 200；校验并记录签名 |
| POST | `/fail` | 500（可用 `/control/fail?enabled=false` 关闭，用于演示重放成功） |
| POST | `/fail/{n}` | 同一 delivery 前 n 次 500，之后 200 |
| POST | `/slow?seconds=5` | 延迟响应（用于租约实验） |
| POST | `/control/fail?enabled=true|false` | 开关 `/fail` |
| GET | `/received?limit=100` | 最近收到的投递（内存保存，重启清空） |
| POST | `/received/reset` | 清空记录 |
| GET | `/health` | 健康检查 |

设置 `RECEIVER_SECRET`（默认 `dev-signing-secret`）后，接收器强制校验签名；
留空则只记录不强制。`RECEIVER_TIMESTAMP_TOLERANCE_SECONDS` 控制时间戳偏差
（默认 300 秒，设为 0 关闭时间戳检查）。

## 配置

全部通过环境变量配置，默认值开箱即用（见 `.env.example`，可复制为 `.env`，
`docker compose` 会自动加载）：

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `DATABASE_URL` | `postgresql://webhook:webhook@db:5432/webhooks` | 数据库 DSN |
| `POSTGRES_USER/PASSWORD/DB/PORT` | `webhook/webhook/webhooks/5432` | db 服务与暴露端口 |
| `API_PORT` / `RECEIVER_PORT` | `8000` / `8001` | 宿主机映射端口 |
| `LEASE_SECONDS` | `30` | 认领租约时长；过期可被接管 |
| `HTTP_TIMEOUT_SECONDS` | `10` | 单次投递 HTTP 超时（应小于租约） |
| `WORKER_POLL_SECONDS` | `1` | 无任务时 Worker 轮询间隔 |
| `MAX_ATTEMPTS` | `6` | 最大尝试次数，达到后进死信 |
| `BACKOFF_BASE_SECONDS` | `5` | 指数退避基数：`base*2^(n-1)` |
| `BACKOFF_CAP_SECONDS` | `300` | 退避间隔上限 |
| `RECEIVER_SECRET` | `dev-signing-secret` | 接收器验签密钥，空则不强制 |
| `RECEIVER_TIMESTAMP_TOLERANCE_SECONDS` | `300` | 签名时间戳允许偏差 |

默认 6 次尝试的退避时刻（相对首次失败）：5s、15s、35s、75s、155s
（间隔 5/10/20/40/80 秒）。联调想快速看到死信，可调小
`BACKOFF_BASE_SECONDS` / `BACKOFF_CAP_SECONDS`。

## 数据库表

- `events`：事件，`(source, event_id)` 唯一。
- `subscriptions`：订阅（source、目标 URL、HMAC 密钥、是否启用）。
- `deliveries`：投递记录。初始投递为 `(chain_id, chain_seq=1)`；重放沿用同一
  `chain_id` 且 `chain_seq` 递增，从而保留完整历史。`seq` 是订阅顺序号。
- `delivery_attempts`：每次尝试的 Worker、结果分类（success/http_error/
  network_error/timeout）、HTTP 状态码、响应摘要（截断 2048 字符）、错误信息、
  租约起止时间。

## 本地开发（不使用 Docker）

```bash
python3 -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt

# 需要一个可连接的 PostgreSQL
export DATABASE_URL=postgresql://user:pass@localhost:5432/webhooks
uvicorn app.main:app --reload --port 8000          # API
python -m app.worker                               # Worker（可起多个）
uvicorn app.receiver:app --port 8001               # 接收器
```

仓库还包含 `test_e2e.py`：用便携 PostgreSQL 拉起完整四进程拓扑，覆盖幂等、
顺序、退避死信、重放、租约接管等场景（开发验证用，非 Docker 依赖）。
