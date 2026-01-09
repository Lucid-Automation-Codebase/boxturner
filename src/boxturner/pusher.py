#!/usr/bin/env python3
"""
Async Pusher Microservice (fail-fast)

Contract:
- sensor1 ↑ :
    * if sensor2 ON -> pusher1 ON (hold)
    * else -> pulse while sensor1 ON, ending OFF
- sensor2 ↓ :
    * if sensor1 ON and pusher1 ON -> pulse until sensor1 OFF
- sensor1 ↓ :
    * always ends with pusher1 OFF (eventually)
"""

from __future__ import annotations

import asyncio
import json
import logging
import sys
import time
from typing import Dict

import zmq
import zmq.asyncio


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] pusher: %(message)s",
)
log = logging.getLogger("pusher")


# =============================================================================
# CONSTANTS
# =============================================================================

DEVICE_ID = "pusher"

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"
GPIO_STATUS_TOPIC = b"gpio.status"

HEARTBEAT_PUB_BIND = "tcp://0.0.0.0:5560"
HEARTBEAT_TOPIC = b"pusher.heartbeat"
HEARTBEAT_HZ = 1.0

GPIO_HEARTBEAT_TIMEOUT_SEC = 5.0

SENSOR_1 = "sensor1"
SENSOR_2 = "sensor2"
PUSHER_1 = "pusher1"

PUSHER_ON_SEC = 0.50
PUSHER_OFF_SEC = 0.40


# =============================================================================
# STATE
# =============================================================================

last_pins: Dict[str, int] | None = None
pusher1_state: int = 0
last_gpio_seen_ts: float | None = None

# REQ/REP sockets must not be used concurrently.
_gpio_lock = asyncio.Lock()


# =============================================================================
# HELPERS
# =============================================================================


def rising_edge(prev: int, curr: int) -> bool:
    return prev == 0 and curr == 1


def falling_edge(prev: int, curr: int) -> bool:
    return prev == 1 and curr == 0


async def gpio_set(
    req_sock: zmq.asyncio.Socket, pin: str, value: bool
) -> None:
    async with _gpio_lock:
        req = {"cmd": "set", "pin": pin, "value": value}
        await req_sock.send_json(req)
        reply = await req_sock.recv_json()
        if reply.get("ok") is not True:
            raise RuntimeError(f"GPIO set failed: {reply}")


async def ensure_pusher_off(req_sock: zmq.asyncio.Socket) -> None:
    global pusher1_state
    if pusher1_state == 0:
        return
    try:
        await gpio_set(req_sock, PUSHER_1, False)
    except asyncio.CancelledError:
        # Best-effort cleanup; do not deadlock shutdown.
        return
    pusher1_state = 0


# =============================================================================
# TASKS
# =============================================================================


async def gpio_listener(
    ctx: zmq.asyncio.Context, gpio_req: zmq.asyncio.Socket
) -> None:
    global last_pins, pusher1_state, last_gpio_seen_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(GPIO_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, GPIO_STATUS_TOPIC)

    log.info("Subscribed to GPIO status")

    pulse_task: asyncio.Task | None = None

    async def pulse_until_sensor1_off() -> None:
        global pusher1_state
        try:
            while last_pins and last_pins[SENSOR_1] == 1:
                await gpio_set(gpio_req, PUSHER_1, True)
                pusher1_state = 1
                await asyncio.sleep(PUSHER_ON_SEC)

                await gpio_set(gpio_req, PUSHER_1, False)
                pusher1_state = 0
                await asyncio.sleep(PUSHER_OFF_SEC)
        except asyncio.CancelledError:
            log.debug("pulse task cancelled")
            raise
        finally:
            await ensure_pusher_off(gpio_req)

    while True:
        topic, raw = await sub.recv_multipart()
        if topic != GPIO_STATUS_TOPIC:
            continue

        last_gpio_seen_ts = time.monotonic()

        msg = json.loads(raw.decode("utf-8"))
        pins = msg["pins"]

        s1 = int(pins[SENSOR_1]["value"])
        s2 = int(pins[SENSOR_2]["value"])

        # ------------------------------------------------------------------
        # Baseline qualification
        # ------------------------------------------------------------------
        if last_pins is None:
            if s1 == 0 and s2 == 0:
                last_pins = {SENSOR_1: s1, SENSOR_2: s2}
                log.info("Baseline established")
            continue

        # ------------------------------------------------------------------
        # sensor1 ↑
        # ------------------------------------------------------------------
        if rising_edge(last_pins[SENSOR_1], s1):
            if pulse_task:
                pulse_task.cancel()
                pulse_task = None
                await ensure_pusher_off(gpio_req)

            if s2 == 1:
                log.info("sensor1 ↑ with sensor2 ON -> HOLD")
                await gpio_set(gpio_req, PUSHER_1, True)
                pusher1_state = 1
            else:
                log.info("sensor1 ↑ with sensor2 OFF -> PULSE")
                pulse_task = asyncio.create_task(pulse_until_sensor1_off())

        # ------------------------------------------------------------------
        # sensor2 ↓
        # ------------------------------------------------------------------
        if falling_edge(last_pins[SENSOR_2], s2):
            if last_pins[SENSOR_1] == 1 and pusher1_state == 1:
                log.info("sensor2 ↓ -> HOLD → PULSE")
                if pulse_task:
                    pulse_task.cancel()
                pulse_task = asyncio.create_task(pulse_until_sensor1_off())

        # ------------------------------------------------------------------
        # sensor1 ↓
        # ------------------------------------------------------------------
        if falling_edge(last_pins[SENSOR_1], s1):
            if pulse_task:
                pulse_task.cancel()
                pulse_task = None
            await ensure_pusher_off(gpio_req)

        last_pins[SENSOR_1] = s1
        last_pins[SENSOR_2] = s2


async def gpio_watchdog() -> None:
    global last_gpio_seen_ts

    while True:
        await asyncio.sleep(0.5)
        if last_gpio_seen_ts is None:
            continue
        if time.monotonic() - last_gpio_seen_ts > GPIO_HEARTBEAT_TIMEOUT_SEC:
            log.critical("GPIO heartbeat lost")
            sys.exit(1)


async def heartbeat_publisher(ctx: zmq.asyncio.Context) -> None:
    pub = ctx.socket(zmq.PUB)
    pub.bind(HEARTBEAT_PUB_BIND)

    period = 1.0 / HEARTBEAT_HZ
    log.info("Heartbeat publisher started")

    while True:
        payload = {
            "type": "heartbeat",
            "device_id": DEVICE_ID,
            "ts_ms": int(time.time() * 1000),
        }
        await pub.send_multipart(
            [
                HEARTBEAT_TOPIC,
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ]
        )
        await asyncio.sleep(period)


# =============================================================================
# MAIN
# =============================================================================


async def _amain() -> None:
    log.info("Pusher service starting")

    ctx = zmq.asyncio.Context.instance()

    gpio_req = ctx.socket(zmq.REQ)
    gpio_req.connect(GPIO_REP_ADDR)

    tasks = [
        asyncio.create_task(
            gpio_listener(ctx, gpio_req), name="gpio_listener"
        ),
        asyncio.create_task(
            heartbeat_publisher(ctx), name="heartbeat_publisher"
        ),
        asyncio.create_task(gpio_watchdog(), name="gpio_watchdog"),
    ]

    done, pending = await asyncio.wait(
        tasks, return_when=asyncio.FIRST_EXCEPTION
    )

    for task in done:
        exc = task.exception()
        if exc is not None:
            log.exception("Fatal task error")
            raise exc

    for task in pending:
        task.cancel()

    await asyncio.gather(*pending, return_exceptions=True)
    ctx.term()


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception:
        log.exception("Service crashed")
        sys.exit(1)


if __name__ == "__main__":
    main()
