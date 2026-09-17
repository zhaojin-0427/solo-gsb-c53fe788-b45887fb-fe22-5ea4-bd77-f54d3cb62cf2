"""HTTP API：事件提交、订阅管理、投递历史、死信与重放。"""
from __future__ import annotations

import asyncpg
from fastapi import APIRouter, HTTPException, Query, Response

from app import repository as repo
from app.db import get_pool
from app.schemas import (
    AttemptOut,
    DeliveryOut,
    EventCreate,
    EventOut,
    ReplayOut,
    SubscriptionCreate,
    SubscriptionOut,
)

router = APIRouter(prefix="/api/v1")

VALID_STATUSES = {"pending", "in_flight", "succeeded", "dead_lettered"}


def _delivery_out(row: asyncpg.Record) -> DeliveryOut:
    data = dict(row)
    return DeliveryOut(
        id=data["id"],
        event_id_fk=data["event_id_fk"],
        subscription_id=data["subscription_id"],
        event_source=data.get("event_source"),
        source_event_id=data.get("source_event_id"),
        target_url=data.get("target_url"),
        chain_id=data["chain_id"],
        chain_seq=data["chain_seq"],
        status=data["status"],
        attempts_made=data["attempts_made"],
        not_before=data["not_before"],
        leased_at=data["leased_at"],
        lease_expires_at=data["lease_expires_at"],
        leased_by=data["leased_by"],
        created_at=data["created_at"],
        updated_at=data["updated_at"],
    )


# -- 事件 ----------------------------------------------------------------------

@router.post("/events", response_model=EventOut, status_code=201, tags=["events"])
async def submit_event(body: EventCreate, response: Response) -> EventOut:
    """提交事件。(source, event_id) 重复时返回原事件且不新增投递（HTTP 200）。"""
    pool = get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            row, duplicate = await repo.submit_event(
                conn, body.source, body.event_id, body.payload
            )
    if duplicate:
        response.status_code = 200
    return EventOut(
        id=row["id"],
        source=row["source"],
        event_id=row["event_id"],
        payload=dict(row["payload"]),
        created_at=row["created_at"],
        duplicate=duplicate,
    )


@router.get("/events/{event_pk}", response_model=EventOut, tags=["events"])
async def get_event(event_pk: int) -> EventOut:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await repo.get_event(conn, event_pk)
    if row is None:
        raise HTTPException(status_code=404, detail="事件不存在")
    return EventOut(
        id=row["id"],
        source=row["source"],
        event_id=row["event_id"],
        payload=dict(row["payload"]),
        created_at=row["created_at"],
    )


@router.get(
    "/events/{event_pk}/deliveries",
    response_model=list[DeliveryOut],
    tags=["events"],
)
async def list_event_deliveries(event_pk: int) -> list[DeliveryOut]:
    pool = get_pool()
    async with pool.acquire() as conn:
        event = await repo.get_event(conn, event_pk)
        if event is None:
            raise HTTPException(status_code=404, detail="事件不存在")
        rows = await repo.list_deliveries_for_event(conn, event_pk)
    return [_delivery_out(r) for r in rows]


# -- 订阅 ----------------------------------------------------------------------

@router.post(
    "/subscriptions",
    response_model=SubscriptionOut,
    status_code=201,
    tags=["subscriptions"],
)
async def create_subscription(body: SubscriptionCreate) -> SubscriptionOut:
    pool = get_pool()
    target = str(body.target_url)
    async with pool.acquire() as conn:
        row = await repo.create_subscription(conn, body.source, target, body.secret)
    return SubscriptionOut(
        id=row["id"],
        source=row["source"],
        target_url=row["target_url"],
        active=row["active"],
        created_at=row["created_at"],
    )


@router.get(
    "/subscriptions", response_model=list[SubscriptionOut], tags=["subscriptions"]
)
async def list_subscriptions(
    source: str | None = Query(default=None),
) -> list[SubscriptionOut]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await repo.list_subscriptions(conn, source)
    return [
        SubscriptionOut(
            id=r["id"],
            source=r["source"],
            target_url=r["target_url"],
            active=r["active"],
            created_at=r["created_at"],
        )
        for r in rows
    ]


@router.get(
    "/subscriptions/{subscription_id}",
    response_model=SubscriptionOut,
    tags=["subscriptions"],
)
async def get_subscription(subscription_id: int) -> SubscriptionOut:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await repo.get_subscription_by_id(conn, subscription_id)
    if row is None:
        raise HTTPException(status_code=404, detail="订阅不存在")
    return SubscriptionOut(
        id=row["id"],
        source=row["source"],
        target_url=row["target_url"],
        active=row["active"],
        created_at=row["created_at"],
    )


# -- 投递历史 / 死信 -------------------------------------------------------------

@router.get(
    "/deliveries", response_model=list[DeliveryOut], tags=["deliveries"]
)
async def list_deliveries(
    status: str | None = Query(default=None),
    subscription_id: int | None = Query(default=None),
    source: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[DeliveryOut]:
    if status is not None and status not in VALID_STATUSES:
        raise HTTPException(
            status_code=400, detail=f"status 必须是 {sorted(VALID_STATUSES)} 之一"
        )
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await repo.list_deliveries(
            conn,
            status=status,
            subscription_id=subscription_id,
            source=source,
            limit=limit,
            offset=offset,
        )
    return [_delivery_out(r) for r in rows]


@router.get(
    "/deliveries/{delivery_id}", response_model=DeliveryOut, tags=["deliveries"]
)
async def get_delivery(delivery_id: int) -> DeliveryOut:
    pool = get_pool()
    async with pool.acquire() as conn:
        row = await repo.get_delivery(conn, delivery_id)
    if row is None:
        raise HTTPException(status_code=404, detail="投递不存在")
    return _delivery_out(row)


@router.get(
    "/deliveries/{delivery_id}/attempts",
    response_model=list[AttemptOut],
    tags=["deliveries"],
)
async def list_delivery_attempts(delivery_id: int) -> list[AttemptOut]:
    pool = get_pool()
    async with pool.acquire() as conn:
        if await repo.get_delivery(conn, delivery_id) is None:
            raise HTTPException(status_code=404, detail="投递不存在")
        rows = await repo.list_attempts(conn, delivery_id)
    return [
        AttemptOut(
            id=r["id"],
            attempt_number=r["attempt_number"],
            worker_id=r["worker_id"],
            started_at=r["started_at"],
            finished_at=r["finished_at"],
            outcome=r["outcome"],
            http_status=r["http_status"],
            response_excerpt=r["response_excerpt"],
            error_message=r["error_message"],
            leased_at=r["leased_at"],
            lease_expires_at=r["lease_expires_at"],
        )
        for r in rows
    ]


@router.get(
    "/dead-letters", response_model=list[DeliveryOut], tags=["dead-letters"]
)
async def list_dead_letters(
    subscription_id: int | None = Query(default=None),
    source: str | None = Query(default=None),
    limit: int = Query(default=100, ge=1, le=500),
    offset: int = Query(default=0, ge=0),
) -> list[DeliveryOut]:
    pool = get_pool()
    async with pool.acquire() as conn:
        rows = await repo.list_deliveries(
            conn,
            status="dead_lettered",
            subscription_id=subscription_id,
            source=source,
            limit=limit,
            offset=offset,
        )
    return [_delivery_out(r) for r in rows]


@router.post(
    "/dead-letters/{delivery_id}/replay",
    response_model=ReplayOut,
    status_code=201,
    tags=["dead-letters"],
)
async def replay_dead_letter(delivery_id: int) -> ReplayOut:
    """重放死信：在同一条投递链上新建投递，旧记录完整保留。"""
    pool = get_pool()
    async with pool.acquire() as conn:
        try:
            new_row = await repo.replay_dead_letter(conn, delivery_id)
        except LookupError:
            raise HTTPException(status_code=404, detail="投递不存在")
        except repo.ReplayError as exc:
            status_code = 409 if exc.code in {"not_dead_lettered", "chain_busy"} else 400
            raise HTTPException(status_code=status_code, detail=str(exc))
    return ReplayOut(
        replayed_from_delivery_id=delivery_id,
        new_delivery_id=new_row["id"],
        chain_id=new_row["chain_id"],
        chain_seq=new_row["chain_seq"],
        status=new_row["status"],
    )
