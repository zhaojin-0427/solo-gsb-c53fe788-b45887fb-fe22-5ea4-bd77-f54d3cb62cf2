"""数据库连接池与表结构初始化。"""
from __future__ import annotations

import asyncio
import json
import logging

import asyncpg

from app.config import settings

log = logging.getLogger("webhook.db")

_pool: asyncpg.Pool | None = None


async def _init_connection(conn: asyncpg.Connection) -> None:
    """让 jsonb 直接以 Python dict 进出。"""
    await conn.set_type_codec(
        "json",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )
    await conn.set_type_codec(
        "jsonb",
        encoder=json.dumps,
        decoder=json.loads,
        schema="pg_catalog",
    )

SCHEMA_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS subscriptions (
        id          BIGSERIAL PRIMARY KEY,
        source      TEXT NOT NULL,
        target_url  TEXT NOT NULL,
        secret      TEXT NOT NULL,
        active      BOOLEAN NOT NULL DEFAULT TRUE,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, target_url)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS events (
        id          BIGSERIAL PRIMARY KEY,
        source      TEXT NOT NULL,
        event_id    TEXT NOT NULL,
        payload     JSONB NOT NULL DEFAULT '{}'::jsonb,
        created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (source, event_id)
    )
    """,
    # 投递链：重放时复用同一 chain_id，chain_seq 从 1 递增；链内严格有序
    "CREATE SEQUENCE IF NOT EXISTS delivery_chain_seq START 1",
    "CREATE SEQUENCE IF NOT EXISTS delivery_seq_global START 1",
    """
    CREATE TABLE IF NOT EXISTS deliveries (
        id               BIGSERIAL PRIMARY KEY,
        event_id_fk      BIGINT NOT NULL REFERENCES events(id),
        subscription_id  BIGINT NOT NULL REFERENCES subscriptions(id),
        chain_id         BIGINT NOT NULL,
        chain_seq        INTEGER NOT NULL,
        seq              BIGINT NOT NULL DEFAULT nextval('delivery_seq_global'),
        status           TEXT NOT NULL CHECK (status IN
                             ('pending', 'in_flight', 'succeeded', 'dead_lettered')),
        attempts_made    INTEGER NOT NULL DEFAULT 0,
        not_before       TIMESTAMPTZ NOT NULL DEFAULT now(),
        leased_at        TIMESTAMPTZ,
        lease_expires_at TIMESTAMPTZ,
        leased_by        TEXT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        UNIQUE (subscription_id, chain_id, chain_seq)
    )
    """,
    # 防止事件首次提交时为同一订阅重复建投递；重放（chain_seq>1）不受此约束。
    # 顺带移除早期版本的全量唯一索引（会阻止重放）。
    "DROP INDEX IF EXISTS uq_deliveries_event_sub",
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_deliveries_event_sub_initial
        ON deliveries (event_id_fk, subscription_id)
        WHERE chain_seq = 1
    """,
    # 顺序判断主索引：按订阅 + seq 找队首
    """
    CREATE INDEX IF NOT EXISTS idx_deliveries_sub_status_seq
        ON deliveries (subscription_id, status, seq)
    """,
    """
    CREATE TABLE IF NOT EXISTS delivery_attempts (
        id               BIGSERIAL PRIMARY KEY,
        delivery_id      BIGINT NOT NULL REFERENCES deliveries(id) ON DELETE CASCADE,
        attempt_number   INTEGER NOT NULL,
        worker_id        TEXT NOT NULL,
        started_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
        finished_at      TIMESTAMPTZ,
        outcome          TEXT NOT NULL CHECK (outcome IN
                             ('success', 'http_error', 'network_error', 'timeout')),
        http_status      INTEGER,
        response_excerpt TEXT,
        error_message    TEXT,
        leased_at        TIMESTAMPTZ,
        lease_expires_at TIMESTAMPTZ,
        UNIQUE (delivery_id, attempt_number)
    )
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_attempts_delivery
        ON delivery_attempts (delivery_id, attempt_number)
    """,
]


async def create_pool(dsn: str | None = None) -> asyncpg.Pool:
    global _pool
    if _pool is not None:
        return _pool

    dsn = dsn or settings.database_url
    # 等待数据库就绪（compose 启动顺序下 postgres 可能尚未接受连接）
    last_error: Exception | None = None
    for attempt in range(60):
        try:
            pool = await asyncpg.create_pool(
                dsn,
                min_size=1,
                max_size=10,
                command_timeout=30,
                init=_init_connection,
            )
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            _pool = pool
            log.info("数据库连接池已建立")
            return pool
        except Exception as exc:  # noqa: BLE001 - 启动期需容忍数据库未就绪
            last_error = exc
            log.info("等待数据库就绪（%s/60）: %s", attempt + 1, exc)
            await asyncio.sleep(1)
    raise RuntimeError(f"无法连接数据库: {last_error}")


async def init_schema(conn: asyncpg.Connection) -> None:
    # 咨询锁串行化多进程（api + 多 worker）启动时的并发 DDL，
    # 避免 IF NOT EXISTS 在 pg_class 上的竞态冲突。
    async with conn.transaction():
        await conn.execute("SELECT pg_advisory_xact_lock(91247)")
        for statement in SCHEMA_STATEMENTS:
            await conn.execute(statement)
    log.info("数据库表结构已初始化")


async def init_db() -> asyncpg.Pool:
    pool = await create_pool()
    async with pool.acquire() as conn:
        await init_schema(conn)
    return pool


def get_pool() -> asyncpg.Pool:
    if _pool is None:
        raise RuntimeError("数据库连接池尚未初始化")
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
