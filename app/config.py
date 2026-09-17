"""运行配置：全部来自环境变量，默认值可直接在 docker compose 中运行。"""
from __future__ import annotations

import os
from dataclasses import dataclass


def _get_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return int(raw)


def _get_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return float(raw)


@dataclass(frozen=True)
class Settings:
    database_url: str = os.getenv(
        "DATABASE_URL", "postgresql://webhook:webhook@db:5432/webhooks"
    )
    # 租约时长：in_flight 投递超过该时间未心跳/未结算即可被其他 Worker 接管
    lease_seconds: int = _get_int("LEASE_SECONDS", 30)
    http_timeout_seconds: float = _get_float("HTTP_TIMEOUT_SECONDS", 10)
    worker_poll_seconds: float = _get_float("WORKER_POLL_SECONDS", 1)
    # 失败策略：共尝试 max_attempts 次；第 n 次失败后等待 base*2^(n-1)（封顶 cap）
    max_attempts: int = _get_int("MAX_ATTEMPTS", 6)
    backoff_base_seconds: float = _get_float("BACKOFF_BASE_SECONDS", 5)
    backoff_cap_seconds: float = _get_float("BACKOFF_CAP_SECONDS", 300)
    # 本地回调接收器
    receiver_secret: str = os.getenv("RECEIVER_SECRET", "")
    receiver_timestamp_tolerance: int = _get_int(
        "RECEIVER_TIMESTAMP_TOLERANCE_SECONDS", 300
    )


settings = Settings()
