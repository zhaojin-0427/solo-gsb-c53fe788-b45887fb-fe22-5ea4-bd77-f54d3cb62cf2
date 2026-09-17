"""API 请求/响应模型。"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field, HttpUrl


class EventCreate(BaseModel):
    source: str = Field(min_length=1, max_length=200, examples=["orders"])
    event_id: str = Field(min_length=1, max_length=200, examples=["evt-1001"])
    payload: dict[str, Any] = Field(default_factory=dict)


class EventOut(BaseModel):
    id: int
    source: str
    event_id: str
    payload: dict[str, Any]
    created_at: datetime
    duplicate: bool = False


class SubscriptionCreate(BaseModel):
    source: str = Field(min_length=1, max_length=200)
    target_url: HttpUrl
    secret: str = Field(min_length=8, max_length=400)


class SubscriptionOut(BaseModel):
    id: int
    source: str
    target_url: str
    active: bool
    created_at: datetime


class AttemptOut(BaseModel):
    id: int
    attempt_number: int
    worker_id: str
    started_at: datetime
    finished_at: datetime | None
    outcome: Literal["success", "http_error", "network_error", "timeout"]
    http_status: int | None
    response_excerpt: str | None
    error_message: str | None
    leased_at: datetime | None
    lease_expires_at: datetime | None


class DeliveryOut(BaseModel):
    id: int
    event_id_fk: int
    subscription_id: int
    event_source: str | None = None
    source_event_id: str | None = None
    target_url: str | None = None
    chain_id: int | None = None
    chain_seq: int
    status: Literal["pending", "in_flight", "succeeded", "dead_lettered"]
    attempts_made: int
    not_before: datetime
    leased_at: datetime | None
    lease_expires_at: datetime | None
    leased_by: str | None
    created_at: datetime
    updated_at: datetime


class ReplayOut(BaseModel):
    replayed_from_delivery_id: int
    new_delivery_id: int
    chain_id: int
    chain_seq: int
    status: str
