#!/usr/bin/env python3
"""
Async Pusher Microservice (fail-fast)

Responsibilities:
- Subscribe to GPIO status (PUB/SUB)
- Send commands to GPIO service (REQ/REP)
- Edge-based logic:
    * sensor1 rising edge AND sensor2 ON  -> pusher1 ON
    * sensor2 falling edge AND pusher1 ON -> pusher1 OFF
- Publish heartbeat (1 Hz) via ZMQ PUB
- Kill process if GPIO heartbeat is lost > 5s
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

# ---- GPIO service -----------------------------------------------------------
GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"
GPIO_STATUS_TOPIC = b"gpio.status"

# ---- Heartbeat --------------------------------------------------------------
HEARTBEAT_PUB_BIND = "tcp://0.0.0.0:5560"
HEARTBEAT_TOPIC = b"pusher.heartbeat"
HEARTBEAT_HZ = 1.0

# ---- Watchdog ---------------------------------------------------------------
GPIO_HEARTBEAT_TIMEOUT_SEC = 5.0

# ---- Pins ------------------------------------------------------------------
SENSOR_1 = "sensor1"
SENSOR_2 = "sensor2"
PUSHER_1 = "pusher1"


# --- Timing ------------------------------------------------------------------
PUSHER_ON_SEC = 0.50
PUSHER_OFF_SEC = 0.40
PUSHER_PULSES = 3

# =============================================================================
# STATE
# =============================================================================

last_pins: Dict[str, int] | None = None
pusher1_state: int = 0
last_gpio_seen_ts: float | None = None


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
    req = {"cmd": "set", "pin": pin, "value": value}
    await req_sock.send_json(req)
    reply = await req_sock.recv_json()
    if reply.get("ok") is not True:
        raise RuntimeError(f"GPIO set failed: {reply}")


# =============================================================================
# TASKS
# =============================================================================


async def gpio_listener(
    ctx: zmq.asyncio.Context,
    gpio_req: zmq.asyncio.Socket,
) -> None:
    global last_pins, pusher1_state, last_gpio_seen_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(GPIO_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, GPIO_STATUS_TOPIC)

    log.info("Subscribed to GPIO status")

    while True:
        topic, raw = await sub.recv_multipart()
        if topic != GPIO_STATUS_TOPIC:
            continue

        last_gpio_seen_ts = time.monotonic()

        msg = json.loads(raw.decode("utf-8"))
        pins = msg["pins"]

        s1 = int(pins[SENSOR_1]["value"])
        s2 = int(pins[SENSOR_2]["value"])

        # ---------------------------------------------------------------------
        # BASELINE QUALIFICATION
        # ---------------------------------------------------------------------
        if last_pins is None:
            if s1 == 0 and s2 == 0:
                last_pins = {
                    SENSOR_1: s1,
                    SENSOR_2: s2,
                }
                log.info("Baseline established (sensors idle)")
            continue

        # ---------------------------------------------------------------------
        # Rule 1:
        # sensor1 ↑ AND sensor2 ON -> pusher1 ON
        # if downstream clear (sensor2 OFF), immediately release
        # ---------------------------------------------------------------------

        if rising_edge(last_pins[SENSOR_1], s1):
            if pusher1_state == 0:
                log.info("sensor1 ↑ -> pusher1 PULSE x3")

                # --- 3 pulses ---
                for i in range(PUSHER_PULSES):
                    log.info("pusher1 ON (pulse %d)", i + 1)
                    await gpio_set(gpio_req, PUSHER_1, True)
                    pusher1_state = 1
                    await asyncio.sleep(PUSHER_ON_SEC)

                    log.info("pusher1 OFF (pulse %d)", i + 1)
                    await gpio_set(gpio_req, PUSHER_1, False)
                    pusher1_state = 0

                    if i < PUSHER_PULSES - 1:
                        await asyncio.sleep(PUSHER_OFF_SEC)

                # --- final state decision ---
                if s2 == 1:
                    log.info(
                        "downstream blocked (sensor2 ON) -> holding pusher1 ON"
                    )
                    await gpio_set(gpio_req, PUSHER_1, True)
                    pusher1_state = 1
                else:
                    log.info(
                        "downstream clear (sensor2 OFF) -> pusher1 remains OFF"
                    )

        # ---------------------------------------------------------------------
        # Rule 2:
        # sensor2 ↓ AND pusher1 ON -> pusher1 OFF
        # ---------------------------------------------------------------------
        if falling_edge(last_pins[SENSOR_2], s2):
            if pusher1_state == 1:
                log.info("sensor2 ↓ and pusher1 ON -> pusher1 OFF")
                await gpio_set(gpio_req, PUSHER_1, False)
                pusher1_state = 0

        last_pins[SENSOR_1] = s1
        last_pins[SENSOR_2] = s2


async def gpio_watchdog() -> None:
    """
    Kill process if GPIO heartbeat disappears.
    """
    global last_gpio_seen_ts

    while True:
        await asyncio.sleep(0.5)

        if last_gpio_seen_ts is None:
            continue

        delta = time.monotonic() - last_gpio_seen_ts
        if delta > GPIO_HEARTBEAT_TIMEOUT_SEC:
            log.critical(
                "GPIO heartbeat lost (%.2fs > %.2fs). Exiting.",
                delta,
                GPIO_HEARTBEAT_TIMEOUT_SEC,
            )
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
        asyncio.create_task(heartbeat_publisher(ctx), name="heartbeat"),
        asyncio.create_task(gpio_watchdog(), name="gpio_watchdog"),
    ]

    done, pending = await asyncio.wait(
        tasks,
        return_when=asyncio.FIRST_EXCEPTION,
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
        log.exception("Service interrupted (KeyboardInterrupt)")
        sys.exit(1)
    except Exception:
        log.exception("Service crashed")
        sys.exit(1)


if __name__ == "__main__":
    main()
