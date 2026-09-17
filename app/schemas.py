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
    current_revision: int
    created_at: datetime


class ConfigVersionPublish(BaseModel):
    """CAS 发布新配置版本。

    target_url/secret 缺省表示沿用当前版本；重试参数缺省表示沿用当前版本
    取值，显式传 null 表示置空（跟随服务全局默认）。
    """

    expected_revision: int = Field(ge=0)
    target_url: HttpUrl | None = None
    secret: str | None = Field(default=None, min_length=8, max_length=400)
    max_attempts: int | None = Field(default=None, ge=1, le=100)
    backoff_base_seconds: float | None = Field(default=None, gt=0, le=86400)
    backoff_cap_seconds: float | None = Field(default=None, gt=0, le=86400)


class ConfigVersionOut(BaseModel):
    """配置版本（永不包含密钥）。边界以订阅内投递 seq 表示。"""

    revision: int
    target_url: str
    max_attempts: int | None
    backoff_base_seconds: float | None
    backoff_cap_seconds: float | None
    is_current: bool
    first_delivery_seq: int | None
    last_delivery_seq: int | None
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
    seq: int | None = None
    status: Literal["pending", "in_flight", "succeeded", "dead_lettered"]
    attempts_made: int
    config_revision: int | None = None
    not_before: datetime
    leased_at: datetime | None
    lease_expires_at: datetime | None
    leased_by: str | None
    created_at: datetime
    updated_at: datetime


class ReplayRequest(BaseModel):
    """重放选项：默认沿用原投递的配置版本；true 改用订阅当前版本。"""

    use_current_config: bool = False


class ReplayOut(BaseModel):
    replayed_from_delivery_id: int
    new_delivery_id: int
    chain_id: int
    chain_seq: int
    status: str
    config_revision: int | None = None
