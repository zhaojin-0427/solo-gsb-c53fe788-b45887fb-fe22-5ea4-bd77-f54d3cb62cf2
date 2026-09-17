"""领域数据访问：事件、订阅、配置版本、投递、投递尝试。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from app.config import settings


class ReplayError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class PublishError(Exception):
    """发布配置版本失败（参数层面，对应 HTTP 400）。"""

    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class RevisionConflictError(PublishError):
    """expected_revision 与当前版本不一致（对应 HTTP 409）。"""

    def __init__(self, current_revision: int):
        super().__init__(
            "revision_conflict",
            f"版本号冲突：当前 revision={current_revision}，"
            "请读取最新版本后以新的 expected_revision 重试",
        )
        self.current_revision = current_revision


class _Unset:
    """区分“字段未提供”（沿用当前版本）与“显式置 null”（跟随全局默认）。"""

    __slots__ = ()

    def __repr__(self) -> str:  # pragma: no cover - 仅调试用
        return "UNSET"


UNSET = _Unset()

# -- 行 -> dict 映射 -----------------------------------------------------------

DELIVERY_COLUMNS = """
    d.id, d.event_id_fk, d.subscription_id, d.chain_id, d.chain_seq, d.seq, d.status,
    d.attempts_made, d.not_before, d.leased_at, d.lease_expires_at, d.leased_by,
    d.created_at, d.updated_at
"""


def delivery_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    data = dict(row)
    # JOIN 出来的展示字段（可能不存在）
    return data


# -- 订阅 ----------------------------------------------------------------------

_SUBSCRIPTION_OUT_COLUMNS = "id, source, target_url, active, current_revision, created_at"


async def _reactivate_or_version(
    conn: asyncpg.Connection, sub_row: asyncpg.Record, target_url: str, secret: str
) -> asyncpg.Record:
    """(source, target_url) 已存在：重新激活；密钥变化时发布一个新版本。

    旧版本保持不可变，已创建投递（含后续重试）继续用旧版本——重新注册不会
    影响在途投递（无损切换）。
    """
    sub_id = sub_row["id"]
    cur = None
    if sub_row["current_config_id"] is not None:
        cur = await conn.fetchrow(
            """
            SELECT secret, max_attempts, backoff_base_seconds, backoff_cap_seconds
            FROM subscription_configs WHERE id = $1
            """,
            sub_row["current_config_id"],
        )
    if cur is not None and cur["secret"] == secret:
        await conn.execute(
            "UPDATE subscriptions SET active = TRUE WHERE id = $1", sub_id
        )
    else:
        next_revision = await conn.fetchval(
            """
            SELECT COALESCE(MAX(revision), 0) + 1
            FROM subscription_configs WHERE subscription_id = $1
            """,
            sub_id,
        )
        cfg = await conn.fetchrow(
            """
            INSERT INTO subscription_configs
                (subscription_id, revision, target_url, secret,
                 max_attempts, backoff_base_seconds, backoff_cap_seconds)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id
            """,
            sub_id,
            next_revision,
            target_url,
            secret,
            cur["max_attempts"] if cur else None,
            cur["backoff_base_seconds"] if cur else None,
            cur["backoff_cap_seconds"] if cur else None,
        )
        await conn.execute(
            """
            UPDATE subscriptions
            SET active = TRUE, secret = $2,
                current_revision = $3, current_config_id = $4
            WHERE id = $1
            """,
            sub_id,
            secret,
            next_revision,
            cfg["id"],
        )
    return await conn.fetchrow(
        f"SELECT {_SUBSCRIPTION_OUT_COLUMNS} FROM subscriptions WHERE id = $1", sub_id
    )


async def create_subscription(
    conn: asyncpg.Connection, source: str, target_url: str, secret: str
) -> asyncpg.Record:
    """新建订阅并创建 revision=1 的初始配置版本。

    (source, target_url) 已存在时重新激活；密钥与当前版本不同则自动发布
    一个新版本（旧投递不受影响）。
    """
    async with conn.transaction():
        row = await conn.fetchrow(
            """
            SELECT id, current_revision, current_config_id
            FROM subscriptions
            WHERE source = $1 AND target_url = $2
            FOR UPDATE
            """,
            source,
            target_url,
        )
        if row is not None:
            return await _reactivate_or_version(conn, row, target_url, secret)
        try:
            # 嵌套 transaction = SAVEPOINT：并发创建撞唯一约束时回滚到此处，
            # 再落入“已存在”分支，而不是让整个请求失败。
            async with conn.transaction():
                sub = await conn.fetchrow(
                    """
                    INSERT INTO subscriptions (source, target_url, secret)
                    VALUES ($1, $2, $3)
                    RETURNING id
                    """,
                    source,
                    target_url,
                    secret,
                )
                cfg = await conn.fetchrow(
                    """
                    INSERT INTO subscription_configs
                        (subscription_id, revision, target_url, secret)
                    VALUES ($1, 1, $2, $3)
                    RETURNING id
                    """,
                    sub["id"],
                    target_url,
                    secret,
                )
                await conn.execute(
                    """
                    UPDATE subscriptions
                    SET current_revision = 1, current_config_id = $2
                    WHERE id = $1
                    """,
                    sub["id"],
                    cfg["id"],
                )
                return await conn.fetchrow(
                    f"SELECT {_SUBSCRIPTION_OUT_COLUMNS} FROM subscriptions WHERE id = $1",
                    sub["id"],
                )
        except asyncpg.UniqueViolationError:
            row = await conn.fetchrow(
                """
                SELECT id, current_revision, current_config_id
                FROM subscriptions
                WHERE source = $1 AND target_url = $2
                FOR UPDATE
                """,
                source,
                target_url,
            )
            return await _reactivate_or_version(conn, row, target_url, secret)


async def get_subscription_by_id(
    conn: asyncpg.Connection, subscription_id: int
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT id, source, target_url, secret, active, current_revision, created_at
        FROM subscriptions WHERE id = $1
        """,
        subscription_id,
    )


async def find_subscription(
    conn: asyncpg.Connection, source: str, target_url: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f"""
        SELECT {_SUBSCRIPTION_OUT_COLUMNS}
        FROM subscriptions WHERE source = $1 AND target_url = $2
        """,
        source,
        target_url,
    )


async def list_subscriptions(
    conn: asyncpg.Connection, source: str | None
) -> list[asyncpg.Record]:
    if source is not None:
        rows = await conn.fetch(
            f"""
            SELECT {_SUBSCRIPTION_OUT_COLUMNS}
            FROM subscriptions WHERE source = $1 ORDER BY id
            """,
            source,
        )
    else:
        rows = await conn.fetch(
            f"""
            SELECT {_SUBSCRIPTION_OUT_COLUMNS}
            FROM subscriptions ORDER BY id
            """
        )
    return list(rows)


# -- 配置版本：CAS 发布与查询 -----------------------------------------------------

async def publish_config_version(
    conn: asyncpg.Connection,
    *,
    subscription_id: int,
    expected_revision: int,
    target_url: str | None = None,
    secret: str | None = None,
    max_attempts: Any = UNSET,
    backoff_base_seconds: Any = UNSET,
    backoff_cap_seconds: Any = UNSET,
) -> asyncpg.Record:
    """以 CAS 发布新的配置版本：仅当 expected_revision 等于当前 revision 时成功。

    与事件提交共用订阅行锁（FOR UPDATE），切换边界在同一事务内原子确定：
    边界前已创建的投递永远固定在旧版本，之后创建的投递使用新版本；
    并发发布只有一个能成功，其余得到 RevisionConflictError。

    target_url/secret 传 None 表示沿用当前版本；重试参数传 UNSET 表示沿用
    当前版本的取值，传 None 表示显式置空（跟随服务全局默认）。
    """
    async with conn.transaction():
        sub = await conn.fetchrow(
            """
            SELECT id, current_revision, current_config_id
            FROM subscriptions WHERE id = $1 FOR UPDATE
            """,
            subscription_id,
        )
        if sub is None:
            raise LookupError("subscription not found")
        if sub["current_revision"] != expected_revision:
            raise RevisionConflictError(sub["current_revision"])

        cur = None
        if sub["current_config_id"] is not None:
            cur = await conn.fetchrow(
                """
                SELECT target_url, secret, max_attempts,
                       backoff_base_seconds, backoff_cap_seconds
                FROM subscription_configs WHERE id = $1
                """,
                sub["current_config_id"],
            )
        new_target = target_url if target_url is not None else (cur["target_url"] if cur else None)
        new_secret = secret if secret is not None else (cur["secret"] if cur else None)
        if new_target is None or new_secret is None:
            raise PublishError(
                "incomplete",
                "订阅尚无当前版本，必须显式提供 target_url 与 secret",
            )

        def _resolve(provided: Any, key: str) -> Any:
            if provided is UNSET:
                return cur[key] if cur else None
            return provided

        new_revision = expected_revision + 1
        cfg = await conn.fetchrow(
            """
            INSERT INTO subscription_configs
                (subscription_id, revision, target_url, secret,
                 max_attempts, backoff_base_seconds, backoff_cap_seconds)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            RETURNING id, revision, target_url, max_attempts,
                      backoff_base_seconds, backoff_cap_seconds, created_at
            """,
            subscription_id,
            new_revision,
            new_target,
            new_secret,
            _resolve(max_attempts, "max_attempts"),
            _resolve(backoff_base_seconds, "backoff_base_seconds"),
            _resolve(backoff_cap_seconds, "backoff_cap_seconds"),
        )
        # 同事务推进当前版本指针；target_url 撞唯一约束时抛
        # UniqueViolationError，由 API 层转为 409。
        await conn.execute(
            """
            UPDATE subscriptions
            SET current_revision = $2, current_config_id = $3,
                target_url = $4, secret = $5
            WHERE id = $1
            """,
            subscription_id,
            new_revision,
            cfg["id"],
            new_target,
            new_secret,
        )
        return cfg


async def list_config_versions(
    conn: asyncpg.Connection, subscription_id: int
) -> list[asyncpg.Record]:
    """列出一个订阅的全部配置版本及切换边界（不含密钥）。

    切换边界以订阅内单调的投递 seq 表示：first_delivery_seq 是该版本生效后
    第一条投递的位置，last_delivery_seq 是最后一条；均为 NULL 表示尚无投递
    使用该版本。
    """
    rows = await conn.fetch(
        """
        SELECT c.id, c.revision, c.target_url,
               c.max_attempts, c.backoff_base_seconds, c.backoff_cap_seconds,
               c.created_at,
               (s.current_config_id = c.id) AS is_current,
               (SELECT MIN(d.seq) FROM deliveries d WHERE d.config_id = c.id)
                   AS first_delivery_seq,
               (SELECT MAX(d.seq) FROM deliveries d WHERE d.config_id = c.id)
                   AS last_delivery_seq
        FROM subscription_configs c
        JOIN subscriptions s ON s.id = c.subscription_id
        WHERE c.subscription_id = $1
        ORDER BY c.revision
        """,
        subscription_id,
    )
    return list(rows)


# -- 事件与投递创建 -------------------------------------------------------------

async def submit_event(
    conn: asyncpg.Connection, source: str, event_id: str, payload: dict[str, Any]
) -> tuple[asyncpg.Record, bool]:
    """幂等写入事件并为该 source 的所有活跃订阅创建投递。

    返回 (event 行, 是否重复提交)。重复提交不新增任何投递。
    """
    event_row = await conn.fetchrow(
        """
        INSERT INTO events (source, event_id, payload)
        VALUES ($1, $2, $3::jsonb)
        ON CONFLICT (source, event_id) DO NOTHING
        RETURNING id, source, event_id, payload, created_at
        """,
        source,
        event_id,
        payload,
    )
    duplicate = event_row is None
    if duplicate:
        event_row = await conn.fetchrow(
            """
            SELECT id, source, event_id, payload, created_at
            FROM events WHERE source = $1 AND event_id = $2
            """,
            source,
            event_id,
        )
        return event_row, True

    # 锁定该 source 的全部活跃订阅（FOR UPDATE）：与发布/重放串行化，
    # 使“与发布竞争的事件归属切换边界哪一侧”在本事务内原子确定——
    # 拿到锁时读到的 current_config_id 就是这批投递永久固定的版本。
    subs = await conn.fetch(
        """
        SELECT id, current_config_id
        FROM subscriptions
        WHERE source = $1 AND active = TRUE
        ORDER BY id
        FOR UPDATE
        """,
        source,
    )
    if subs:
        await conn.execute(
            """
            INSERT INTO deliveries
                (event_id_fk, subscription_id, chain_id, chain_seq, status, config_id)
            SELECT $1, s.subscription_id, nextval('delivery_chain_seq'), 1,
                   'pending', s.config_id
            FROM unnest($2::bigint[], $3::bigint[]) AS s(subscription_id, config_id)
            """,
            event_row["id"],
            [r["id"] for r in subs],
            [r["current_config_id"] for r in subs],
        )
    return event_row, False


async def get_event(
    conn: asyncpg.Connection, event_pk: int
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT id, source, event_id, payload, created_at
        FROM events WHERE id = $1
        """,
        event_pk,
    )


# -- Worker：认领队首任务 --------------------------------------------------------

# 严格顺序的关键：
# 1) 先在事务中锁定该订阅“可投递且 seq 最小”的那一条（FOR UPDATE SKIP LOCKED）；
# 2) 检查它前面是否还有未终结的投递，有则跳过（保证前一事件成功或进死信后才动）；
# 3) 用 lease_token 栅栏把它置为 in_flight，旧租约必须已过期才可接管。
async def claim_delivery(
    conn: asyncpg.Connection, worker_id: str
) -> dict[str, Any] | None:
    now = datetime.now(timezone.utc)
    lease_seconds = settings.lease_seconds
    lease_expires = now + timedelta(seconds=lease_seconds)
    lease_token = f"{worker_id}:{now.timestamp()}"

    async with conn.transaction():
        # 候选：每个订阅 seq 最小的可处理投递（pending 到点 或 租约过期的 in_flight）。
        # NOT EXISTS 保证严格顺序：前面还有未终结投递时不允许发送后续事件。
        candidate = await conn.fetchrow(
            """
            SELECT d.id, d.event_id_fk, d.subscription_id, d.status,
                   d.attempts_made, d.leased_by
            FROM deliveries d
            WHERE (
                    (d.status = 'pending' AND d.not_before <= $1)
                 OR (d.status = 'in_flight' AND d.lease_expires_at < $1)
                )
              AND NOT EXISTS (
                    SELECT 1
                    FROM subscriptions s
                    WHERE s.id = d.subscription_id AND s.active = FALSE
              )
              AND NOT EXISTS (
                    SELECT 1
                    FROM deliveries earlier
                    WHERE earlier.subscription_id = d.subscription_id
                      AND earlier.seq < d.seq
                      AND earlier.status IN ('pending', 'in_flight')
              )
            ORDER BY d.seq
            LIMIT 1
            FOR UPDATE OF d SKIP LOCKED
            """,
            now,
        )
        if candidate is None:
            return None

        updated = await conn.fetchrow(
            """
            UPDATE deliveries
            SET status = 'in_flight',
                leased_at = $2,
                lease_expires_at = $3,
                leased_by = $4,
                updated_at = $2
            WHERE id = $1
            RETURNING id, event_id_fk, subscription_id, chain_id, chain_seq,
                      attempts_made, status, not_before
            """,
            candidate["id"],
            now,
            lease_expires,
            lease_token,
        )
        if updated is None:
            return None

        return {
            "delivery": updated,
            "lease_token": lease_token,
            "leased_at": now,
            "lease_expires_at": lease_expires,
        }


async def load_delivery_context(
    conn: asyncpg.Connection, delivery_id: int
) -> asyncpg.Record | None:
    # 投递使用的 target_url/密钥/重试参数来自创建时固定的配置版本
    # （deliveries.config_id），保证重试不会因版本切换而改用另一版本。
    # config_id 为 NULL 的历史/外部数据回退到订阅行当前值（旧行为）。
    return await conn.fetchrow(
        """
        SELECT d.id AS delivery_id,
               d.attempts_made,
               e.id AS event_pk,
               e.source,
               e.event_id,
               e.payload,
               s.id AS subscription_pk,
               COALESCE(c.target_url, s.target_url) AS target_url,
               COALESCE(c.secret, s.secret) AS secret,
               c.max_attempts,
               c.backoff_base_seconds,
               c.backoff_cap_seconds,
               c.revision AS config_revision
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
        LEFT JOIN subscription_configs c ON c.id = d.config_id
        WHERE d.id = $1
        """,
        delivery_id,
    )


# -- Worker：记录尝试并结算 ------------------------------------------------------

async def record_attempt(
    conn: asyncpg.Connection,
    *,
    delivery_id: int,
    lease_token: str,
    worker_id: str,
    outcome: str,
    http_status: int | None,
    response_excerpt: str | None,
    error_message: str | None,
    leased_at: datetime,
    lease_expires_at: datetime,
) -> int | None:
    """在租约持有方身份校验通过后追加一条尝试记录并 attempts_made+1。

    返回新的 attempt_number；若租约已被接管则返回 None（本次结果作废）。
    """
    async with conn.transaction():
        locked = await conn.fetchrow(
            """
            SELECT leased_by FROM deliveries
            WHERE id = $1 FOR UPDATE
            """,
            delivery_id,
        )
        if locked is None or locked["leased_by"] != lease_token:
            return None

        attempt_number = await conn.fetchval(
            """
            INSERT INTO delivery_attempts
                (delivery_id, attempt_number, worker_id, outcome, http_status,
                 response_excerpt, error_message, leased_at, lease_expires_at,
                 finished_at)
            SELECT $1, COALESCE(MAX(attempt_number), 0) + 1, $2, $3, $4, $5, $6,
                   $7, $8, now()
            FROM delivery_attempts
            WHERE delivery_id = $1
            RETURNING attempt_number
            """,
            delivery_id,
            worker_id,
            outcome,
            http_status,
            response_excerpt,
            error_message,
            leased_at,
            lease_expires_at,
        )
        await conn.execute(
            "UPDATE deliveries SET attempts_made = attempts_made + 1, updated_at = now() WHERE id = $1",
            delivery_id,
        )
        return attempt_number


async def settle_delivery(
    conn: asyncpg.Connection,
    *,
    delivery_id: int,
    lease_token: str,
    succeeded: bool,
    not_before: datetime | None,
    max_attempts: int,
) -> str | None:
    """根据本次尝试结果结算。

    成功 -> succeeded；失败且已达 max_attempts（投递固定版本的重试参数）
    -> dead_lettered；否则回到 pending 并设置指数退避时间。
    返回结算后的状态；租约失效返回 None。
    """
    async with conn.transaction():
        locked = await conn.fetchrow(
            """
            SELECT attempts_made, leased_by FROM deliveries
            WHERE id = $1 FOR UPDATE
            """,
            delivery_id,
        )
        if locked is None or locked["leased_by"] != lease_token:
            return None

        if succeeded:
            status = "succeeded"
        elif locked["attempts_made"] >= max_attempts:
            status = "dead_lettered"
        else:
            status = "pending"

        await conn.execute(
            """
            UPDATE deliveries
            SET status = $2,
                not_before = COALESCE($3, now()),
                leased_at = NULL,
                lease_expires_at = NULL,
                leased_by = NULL,
                updated_at = now()
            WHERE id = $1
            """,
            delivery_id,
            status,
            not_before,
        )
        return status


# -- 查询：历史 / 死信 -----------------------------------------------------------

async def list_deliveries(
    conn: asyncpg.Connection,
    *,
    status: str | None = None,
    subscription_id: int | None = None,
    source: str | None = None,
    limit: int = 100,
    offset: int = 0,
) -> list[asyncpg.Record]:
    rows = await conn.fetch(
        f"""
        SELECT {DELIVERY_COLUMNS},
               e.source AS event_source,
               e.event_id AS source_event_id,
               s.target_url AS target_url,
               c.revision AS config_revision
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
        LEFT JOIN subscription_configs c ON c.id = d.config_id
        WHERE ($1::text IS NULL OR d.status = $1)
          AND ($2::bigint IS NULL OR d.subscription_id = $2)
          AND ($3::text IS NULL OR e.source = $3)
        ORDER BY d.id DESC
        LIMIT $4 OFFSET $5
        """,
        status,
        subscription_id,
        source,
        limit,
        offset,
    )
    return list(rows)


async def get_delivery(
    conn: asyncpg.Connection, delivery_id: int
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        f"""
        SELECT {DELIVERY_COLUMNS},
               e.source AS event_source,
               e.event_id AS source_event_id,
               s.target_url AS target_url,
               c.revision AS config_revision
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
        LEFT JOIN subscription_configs c ON c.id = d.config_id
        WHERE d.id = $1
        """,
        delivery_id,
    )


async def list_attempts(
    conn: asyncpg.Connection, delivery_id: int
) -> list[asyncpg.Record]:
    rows = await conn.fetch(
        """
        SELECT id, attempt_number, worker_id, started_at, finished_at, outcome,
               http_status, response_excerpt, error_message,
               leased_at, lease_expires_at
        FROM delivery_attempts
        WHERE delivery_id = $1
        ORDER BY attempt_number
        """,
        delivery_id,
    )
    return list(rows)


async def list_deliveries_for_event(
    conn: asyncpg.Connection, event_pk: int
) -> list[asyncpg.Record]:
    rows = await conn.fetch(
        f"""
        SELECT {DELIVERY_COLUMNS},
               e.source AS event_source,
               e.event_id AS source_event_id,
               s.target_url AS target_url,
               c.revision AS config_revision
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
        LEFT JOIN subscription_configs c ON c.id = d.config_id
        WHERE d.event_id_fk = $1
        ORDER BY d.id
        """,
        event_pk,
    )
    return list(rows)


# -- 死信重放 --------------------------------------------------------------------

async def replay_dead_letter(
    conn: asyncpg.Connection, delivery_id: int, *, use_current_config: bool = False
) -> tuple[asyncpg.Record, int | None]:
    """为死信投递在链尾新建一条投递链记录（同 chain_id，chain_seq 递增），保留旧记录。

    默认沿用原投递固定的配置版本；use_current_config=True 时改用订阅当前
    版本。选择结果随新投递记录固化（config_id 不再改变）。
    返回 (新投递行, 新投递固定的 config_revision)。

    原投递为非死信状态时抛 ReplayError(not_dead_lettered)；
    链上已有活动投递时抛 ReplayError(chain_busy)。
    """
    async with conn.transaction():
        original = await conn.fetchrow(
            """
            SELECT id, event_id_fk, subscription_id, chain_id, chain_seq,
                   status, config_id
            FROM deliveries WHERE id = $1
            """,
            delivery_id,
        )
        if original is None:
            raise LookupError("delivery not found")

        # 锁顺序与事件提交一致（先订阅行后投递行），并与发布串行化：
        # “当前版本”在本事务内是唯一确定的。
        sub = await conn.fetchrow(
            """
            SELECT id, current_config_id
            FROM subscriptions WHERE id = $1 FOR UPDATE
            """,
            original["subscription_id"],
        )

        locked = await conn.fetchrow(
            "SELECT status FROM deliveries WHERE id = $1 FOR UPDATE",
            delivery_id,
        )
        if locked["status"] != "dead_lettered":
            raise ReplayError("not_dead_lettered", "只有死信状态的投递可以重放")

        busy = await conn.fetchval(
            """
            SELECT 1 FROM deliveries
            WHERE subscription_id = $1
              AND chain_id = $2
              AND status IN ('pending', 'in_flight')
            LIMIT 1
            """,
            original["subscription_id"],
            original["chain_id"],
        )
        if busy is not None:
            raise ReplayError("chain_busy", "该重放链上已有待投递/投递中的任务")

        if use_current_config or original["config_id"] is None:
            config_id = sub["current_config_id"]
        else:
            config_id = original["config_id"]

        # 追加到链尾（而非 original.chain_seq + 1）：同一死信被多次重放、
        # 或重放链上较旧的记录时，新记录始终落在链尾，不会撞
        # (subscription_id, chain_id, chain_seq) 唯一约束。
        # 重放都持有订阅行锁，MAX 在本事务内稳定。
        next_chain_seq = await conn.fetchval(
            """
            SELECT COALESCE(MAX(chain_seq), 0) + 1
            FROM deliveries
            WHERE subscription_id = $1 AND chain_id = $2
            """,
            original["subscription_id"],
            original["chain_id"],
        )

        new_row = await conn.fetchrow(
            """
            INSERT INTO deliveries
                (event_id_fk, subscription_id, chain_id, chain_seq, status, config_id)
            VALUES ($1, $2, $3, $4, 'pending', $5)
            RETURNING id, event_id_fk, subscription_id, chain_id, chain_seq,
                      status, attempts_made, not_before, leased_at,
                      lease_expires_at, leased_by, created_at, updated_at
            """,
            original["event_id_fk"],
            original["subscription_id"],
            original["chain_id"],
            next_chain_seq,
            config_id,
        )
        revision = None
        if config_id is not None:
            revision = await conn.fetchval(
                "SELECT revision FROM subscription_configs WHERE id = $1", config_id
            )
        return new_row, revision
