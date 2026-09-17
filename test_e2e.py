"""端到端验证脚本（非 Docker 环境使用）：启动便携 Postgres、API、Receiver、
两个 Worker，覆盖：
1. 事件幂等（重复提交返回原事件且不新增投递）
2. 成功投递 + HMAC 校验
3. 严格顺序（失败阻塞时后一事件不被发送）
4. 6 次尝试后死信（指数退避）
5. 死信重放生成新链记录、旧记录保留
6. 租约超时接管
"""
from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pgserver

ROOT = Path(__file__).resolve().parent
VENV_PY = os.environ.get("VENV_PY", "/tmp/venv/bin/python")

# 用极短退避加速测试：1s, 2s, 4s, 8s, 8s(cap)
COMMON_ENV = {
    "LEASE_SECONDS": "3",
    "HTTP_TIMEOUT_SECONDS": "2",
    "WORKER_POLL_SECONDS": "0.2",
    "MAX_ATTEMPTS": "6",
    "BACKOFF_BASE_SECONDS": "1",
    "BACKOFF_CAP_SECONDS": "8",
    "RECEIVER_SECRET": "dev-signing-secret",
    "RECEIVER_TIMESTAMP_TOLERANCE_SECONDS": "600",
    "PYTHONUNBUFFERED": "1",
}


def free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def wait_http(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            if httpx.get(url, timeout=1).status_code == 200:
                return
        except Exception as exc:  # noqa: BLE001
            last = exc
        time.sleep(0.3)
    raise RuntimeError(f"服务未就绪: {url} ({last})")


def wait_for(condition, desc: str, timeout: float = 60.0, interval: float = 0.5):
    deadline = time.time() + timeout
    while time.time() < deadline:
        result = condition()
        if result:
            return result
        time.sleep(interval)
    raise AssertionError(f"等待超时: {desc}")


class Harness:
    def __init__(self) -> None:
        self.procs: list[subprocess.Popen] = []

    def spawn(self, module_cmd: list[str], port: int | None, name: str, extra_env: dict | None = None):
        env = os.environ.copy()
        env.update(COMMON_ENV)
        env["DATABASE_URL"] = self.dsn
        if extra_env:
            env.update(extra_env)
        log = open(ROOT / f"test-{name}.log", "w")
        proc = subprocess.Popen(
            [VENV_PY, "-m", *module_cmd],
            cwd=ROOT,
            env=env,
            stdout=log,
            stderr=subprocess.STDOUT,
        )
        proc.name = name  # type: ignore[attr-defined]
        self.procs.append(proc)
        return proc

    def start(self):
        self.pg = pgserver.get_server(
            str(ROOT / ".test-pgdata"), cleanup_mode="stop"
        )
        self.dsn = self.pg.get_uri()
        print("Postgres DSN:", self.dsn)

        api_port = free_port()
        rcv_port = free_port()
        self.api = f"http://127.0.0.1:{api_port}"
        self.rcv = f"http://127.0.0.1:{rcv_port}"

        self.spawn(
            ["uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(api_port)],
            api_port, "api",
        )
        self.spawn(
            ["uvicorn", "app.receiver:app", "--host", "127.0.0.1", "--port", str(rcv_port)],
            rcv_port, "receiver",
        )
        self.spawn(["app.worker"], None, "worker1")
        self.spawn(["app.worker"], None, "worker2")
        wait_http(f"{self.api}/health")
        wait_http(f"{self.rcv}/health")
        print("API/Receiver/Workers 已启动")

    def stop(self):
        for p in self.procs:
            p.send_signal(signal.SIGTERM)
        for p in self.procs:
            try:
                p.wait(timeout=8)
            except subprocess.TimeoutExpired:
                p.kill()
        try:
            self.pg.cleanup()
        except Exception:  # noqa: BLE001
            pass


def main() -> int:
    h = Harness()
    h.start()
    failures = []

    def check(name: str, ok: bool, detail: str = ""):
        print(f"[{'PASS' if ok else 'FAIL'}] {name} {detail}")
        if not ok:
            failures.append(name)

    try:
        api = httpx.Client(base_url=h.api, timeout=5)
        rcv = httpx.Client(base_url=h.rcv, timeout=10)

        rcv.post("/received/reset")

        # 1) 幂等：先建订阅，再重复提交同一事件
        sub = api.post("/api/v1/subscriptions", json={
            "source": "orders",
            "target_url": f"{h.rcv}/receive",
            "secret": "dev-signing-secret",
        }).json()
        check("创建订阅", sub["active"] is True, json.dumps(sub, ensure_ascii=False))

        ev1 = api.post("/api/v1/events", json={
            "source": "orders", "event_id": "evt-1", "payload": {"n": 1},
        })
        check("首次提交事件 201", ev1.status_code == 201 and ev1.json()["duplicate"] is False)
        ev1b = api.post("/api/v1/events", json={
            "source": "orders", "event_id": "evt-1", "payload": {"n": 999},
        })
        check("重复提交 200 + duplicate", ev1b.status_code == 200 and ev1b.json()["duplicate"] is True)
        check(
            "重复提交返回原事件且不覆盖 payload",
            ev1b.json()["payload"] == {"n": 1} and ev1b.json()["id"] == ev1.json()["id"],
        )

        # 2) 成功投递
        d = wait_for(
            lambda: next((x for x in api.get("/api/v1/deliveries",
                        params={"source": "orders"}).json()
                         if x["source_event_id"] == "evt-1"), None),
            "evt-1 投递出现",
        )
        wait_for(lambda: api.get(f"/api/v1/deliveries/{d['id']}").json()["status"] == "succeeded",
                 "evt-1 投递成功")
        d = api.get(f"/api/v1/deliveries/{d['id']}").json()
        check("evt-1 一次成功", d["attempts_made"] == 1 and d["status"] == "succeeded")

        recv_items = rcv.get("/received").json()["items"]
        evt1_rec = next(x for x in recv_items if x["event_id"] == "evt-1")
        check("Receiver 校验签名通过", evt1_rec["signature_valid"] is True, evt1_rec["signature_message"])

        # 3) 严格顺序：/block 对 evt-seq-1 始终 500；evt-seq-2 不能在其终结前发出
        api.post("/api/v1/subscriptions", json={
            "source": "seq",
            "target_url": f"{h.rcv}/fail",
            "secret": "dev-signing-secret",
        })
        r1 = api.post("/api/v1/events", json={
            "source": "seq", "event_id": "seq-1", "payload": {},
        }).json()
        r2 = api.post("/api/v1/events", json={
            "source": "seq", "event_id": "seq-2", "payload": {},
        }).json()

        def deliveries_of(event_pk):
            return api.get(f"/api/v1/events/{event_pk}/deliveries").json()

        def dl1_dl2():
            dl1 = deliveries_of(r1["id"])
            dl2 = deliveries_of(r2["id"])
            return (dl1[0] if dl1 else None), (dl2[0] if dl2 else None)

        dl1, dl2 = wait_for(lambda: dl1_dl2() if all(dl1_dl2()) else None, "两条投递均创建")

        # 等 dl1 出现多次尝试（说明它在反复失败），期间 dl2 必须保持 pending/0 次
        wait_for(
            lambda: api.get(f"/api/v1/deliveries/{dl1['id']}").json()["attempts_made"] >= 2,
            "seq-1 已失败 >=2 次", timeout=30,
        )
        dl2_now = api.get(f"/api/v1/deliveries/{dl2['id']}").json()
        check(
            "严格顺序：前序未终结时后序不发送",
            dl2_now["attempts_made"] == 0 and dl2_now["status"] == "pending",
            f"seq-2 status={dl2_now['status']} attempts={dl2_now['attempts_made']}",
        )

        # 4) 6 次尝试后死信（1,2,4,8,8 退避 ≈ 23s）
        wait_for(
            lambda: api.get(f"/api/v1/deliveries/{dl1['id']}").json()["status"] == "dead_lettered",
            "seq-1 进入死信", timeout=90,
        )
        dl1_final = api.get(f"/api/v1/deliveries/{dl1['id']}").json()
        check("死信恰好 6 次尝试", dl1_final["attempts_made"] == 6)
        attempts = api.get(f"/api/v1/deliveries/{dl1['id']}/attempts").json()
        check("历史含 6 条尝试记录", len(attempts) == 6 and attempts[0]["attempt_number"] == 1)

        # 前序进入死信后，seq-2 才开始处理并同样最终进死信
        wait_for(
            lambda: api.get(f"/api/v1/deliveries/{dl2['id']}").json()["attempts_made"] >= 1,
            "前序死信后 seq-2 才开始", timeout=10,
        )
        check("严格顺序：前序死信后后序才开始", True)

        # 5) 死信列表 + 重放（关闭 /fail 后重放成功，生成新链记录）
        dls = api.get("/api/v1/dead-letters", params={"source": "seq"}).json()
        check("死信列表可查", any(x["id"] == dl1["id"] for x in dls))

        bad = api.post(f"/api/v1/dead-letters/{d['id']}/replay")
        check("非死信重放返回 409", bad.status_code == 409)

        ctl = rcv.post("/control/fail", params={"enabled": False})
        ctl.raise_for_status()
        replay = api.post(f"/api/v1/dead-letters/{dl1['id']}/replay")
        check("重放返回 201", replay.status_code == 201, replay.text)
        new_id = replay.json()["new_delivery_id"]
        wait_for(
            lambda: api.get(f"/api/v1/deliveries/{new_id}").json()["status"] == "succeeded",
            "重放投递成功", timeout=20,
        )
        old = api.get(f"/api/v1/deliveries/{dl1['id']}").json()
        new = api.get(f"/api/v1/deliveries/{new_id}").json()
        check("重放为同链新记录(chain_seq+1)且旧记录保留",
              old["status"] == "dead_lettered"
              and new["chain_id"] == old["chain_id"]
              and new["chain_seq"] == old["chain_seq"] + 1
              and new["attempts_made"] == 1)

        # 链忙时重复重放 -> 409（先制造一个死信：seq-2 此刻可能仍在失败；
        # 用它在 pending 窗口小概率，改做直接规则校验：对成功的新记录重放 -> 409）
        again = api.post(f"/api/v1/dead-letters/{new_id}/replay")
        check("重放非死信记录返回 409", again.status_code == 409)

        # 6) 租约接管：模拟宕机 Worker——直接写入一条 in_flight 且租约已过期的投递。
        # 存活 Worker 应在 lease 过期后接管并完成投递。
        import asyncio

        import asyncpg

        async def seed_stale_lease():
            conn = await asyncpg.connect(h.dsn)
            await conn.set_type_codec(
                "jsonb", encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
            )
            ev = await conn.fetchrow(
                """
                INSERT INTO events (source, event_id, payload)
                VALUES ('lease', 'lease-1', '{}'::jsonb)
                RETURNING id
                """
            )
            sub = await conn.fetchrow(
                """
                INSERT INTO subscriptions (source, target_url, secret)
                VALUES ('lease', $1, 'dev-signing-secret')
                ON CONFLICT (source, target_url) DO UPDATE SET active = TRUE
                RETURNING id
                """,
                f"{h.rcv}/receive",
            )
            row = await conn.fetchrow(
                """
                INSERT INTO deliveries
                    (event_id_fk, subscription_id, chain_id, chain_seq,
                     status, attempts_made, not_before,
                     leased_at, lease_expires_at, leased_by)
                VALUES ($1, $2, nextval('delivery_chain_seq'), 1,
                        'in_flight', 0, now() - interval '1 hour',
                        now() - interval '1 minute',
                        now() - interval '57 seconds',
                        'dead-worker')
                RETURNING id
                """,
                ev["id"],
                sub["id"],
            )
            await conn.close()
            return ev["id"], row["id"]

        lease_event, dl_lease = asyncio.run(seed_stale_lease())
        wait_for(
            lambda: api.get(f"/api/v1/deliveries/{dl_lease}").json()["status"] == "succeeded",
            "过期租约被接管并成功", timeout=30,
        )
        lease_d = api.get(f"/api/v1/deliveries/{dl_lease}").json()
        lease_attempts = api.get(f"/api/v1/deliveries/{dl_lease}/attempts").json()
        check("租约超时后被存活 Worker 接管",
              lease_d["status"] == "succeeded"
              and lease_attempts
              and lease_attempts[0]["worker_id"] != "dead-worker",
              f"worker={lease_attempts[0]['worker_id'] if lease_attempts else None}")

    except Exception as exc:  # noqa: BLE001
        import traceback
        traceback.print_exc()
        failures.append(f"异常: {exc}")
    finally:
        h.stop()

    print()
    if failures:
        print(f"失败 {len(failures)} 项: {failures}")
        return 1
    print("全部端到端检查通过 ✅")
    return 0


if __name__ == "__main__":
    sys.exit(main())
