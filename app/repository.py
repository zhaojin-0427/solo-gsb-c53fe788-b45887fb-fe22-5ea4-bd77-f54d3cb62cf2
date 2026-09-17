"""领域数据访问：事件、订阅、投递、投递尝试。"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import asyncpg

from app.config import settings


class ReplayError(Exception):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code

# -- 行 -> dict 映射 -----------------------------------------------------------

DELIVERY_COLUMNS = """
    d.id, d.event_id_fk, d.subscription_id, d.chain_id, d.chain_seq, d.status,
    d.attempts_made, d.not_before, d.leased_at, d.lease_expires_at, d.leased_by,
    d.created_at, d.updated_at
"""


def delivery_to_dict(row: asyncpg.Record) -> dict[str, Any]:
    data = dict(row)
    # JOIN 出来的展示字段（可能不存在）
    return data


# -- 订阅 ----------------------------------------------------------------------

async def create_subscription(
    conn: asyncpg.Connection, source: str, target_url: str, secret: str
) -> asyncpg.Record:
    """新建订阅；(source, target_url) 已存在时重新激活并返回原行。"""
    return await conn.fetchrow(
        """
        INSERT INTO subscriptions (source, target_url, secret)
        VALUES ($1, $2, $3)
        ON CONFLICT (source, target_url) DO UPDATE
            SET active = TRUE, secret = EXCLUDED.secret
        RETURNING id, source, target_url, active, created_at
        """,
        source,
        target_url,
        secret,
    )


async def get_subscription_by_id(
    conn: asyncpg.Connection, subscription_id: int
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT id, source, target_url, secret, active, created_at
        FROM subscriptions WHERE id = $1
        """,
        subscription_id,
    )


async def find_subscription(
    conn: asyncpg.Connection, source: str, target_url: str
) -> asyncpg.Record | None:
    return await conn.fetchrow(
        """
        SELECT id, source, target_url, active, created_at
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
            """
            SELECT id, source, target_url, active, created_at
            FROM subscriptions WHERE source = $1 ORDER BY id
            """,
            source,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, source, target_url, active, created_at
            FROM subscriptions ORDER BY id
            """
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

    await conn.execute(
        """
        INSERT INTO deliveries
            (event_id_fk, subscription_id, chain_id, chain_seq, status)
        SELECT e.id, s.id, nextval('delivery_chain_seq'), 1, 'pending'
        FROM events e, subscriptions s
        WHERE e.id = $1 AND s.source = $2 AND s.active = TRUE
        """,
        event_row["id"],
        source,
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
    return await conn.fetchrow(
        """
        SELECT d.id AS delivery_id,
               d.attempts_made,
               e.id AS event_pk,
               e.source,
               e.event_id,
               e.payload,
               s.id AS subscription_pk,
               s.target_url,
               s.secret
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
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
) -> str | None:
    """根据本次尝试结果结算。

    成功 -> succeeded；失败且已达 max_attempts -> dead_lettered；否则回到 pending
    并设置指数退避时间。返回结算后的状态；租约失效返回 None。
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
        elif locked["attempts_made"] >= settings.max_attempts:
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
               s.target_url AS target_url
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
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
               s.target_url AS target_url
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
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
               s.target_url AS target_url
        FROM deliveries d
        JOIN events e ON e.id = d.event_id_fk
        JOIN subscriptions s ON s.id = d.subscription_id
        WHERE d.event_id_fk = $1
        ORDER BY d.id
        """,
        event_pk,
    )
    return list(rows)


# -- 死信重放 --------------------------------------------------------------------

async def replay_dead_letter(
    conn: asyncpg.Connection, delivery_id: int
) -> asyncpg.Record:
    """为死信投递新建一条投递链记录（同 chain_id，chain_seq+1），保留旧记录。

    原投递为非死信状态时抛 RaiseError(DLX)；链上已有活动投递时抛 RaiseError(BUSY)。
    """
    async with conn.transaction():
        original = await conn.fetchrow(
            """
            SELECT id, event_id_fk, subscription_id, chain_id, chain_seq, status
            FROM deliveries WHERE id = $1 FOR UPDATE
            """,
            delivery_id,
        )
        if original is None:
            raise LookupError("delivery not found")
        if original["status"] != "dead_lettered":
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

        new_row = await conn.fetchrow(
            """
            INSERT INTO deliveries
                (event_id_fk, subscription_id, chain_id, chain_seq, status)
            VALUES ($1, $2, $3, $4, 'pending')
            RETURNING id, event_id_fk, subscription_id, chain_id, chain_seq,
                      status, attempts_made, not_before, leased_at,
                      lease_expires_at, leased_by, created_at, updated_at
            """,
            original["event_id_fk"],
            original["subscription_id"],
            original["chain_id"],
            original["chain_seq"] + 1,
        )
        return new_row
