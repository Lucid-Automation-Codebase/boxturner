#!/usr/bin/env python3
"""
Main Sequencer Service (fail-fast)

Responsibilities:
- Verify GPIO and Pusher heartbeats on startup
- Coordinate arm / vacuum / pusher2
- Execute measurement pipeline
- Handle queued boxes correctly
- Publish reduced machine state via MQTT (async, aiomqtt)
- Kill process if GPIO or Pusher heartbeat is lost
- Detect missing box between pusher2 release and sensor3 rise
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import sys
import time
from typing import Any, Dict

import aiomqtt
import zmq
import zmq.asyncio


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] main: %(message)s",
)
log = logging.getLogger("main")


# =============================================================================
# ZMQ CONFIG
# =============================================================================

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_STATUS_TOPIC = b"gpio.status"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"

PUSHER_PUB_ADDR = "tcp://127.0.0.1:5560"
PUSHER_HEARTBEAT_TOPIC = b"pusher.heartbeat"


# =============================================================================
# MQTT CONFIG
# =============================================================================

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC_STATE = "lucid/boxturner/state"


# =============================================================================
# TIMEOUTS
# =============================================================================

STARTUP_TIMEOUT_SEC = 3.0
RECV_TIMEOUT_MS = 500
HEALTH_TIMEOUT_SEC = 5.0
BOX_MISSING_TIMEOUT_SEC = 10.0


# =============================================================================
# TIMING CONFIG
# =============================================================================

ARM_SETTLE_DELAY_SEC = 0.5  # Time for arm to move into position
VACUUM_HOLD_DELAY_SEC = 1.0
POST_MEASUREMENT_DELAY_SEC = 0.5
RUNNING_RESET_DELAY_SEC = 1.0  # After sensor3 down, time to running=False


# =============================================================================
# PINS
# =============================================================================

SENSOR2 = "sensor2"
SENSOR3 = "sensor3"

PUSHER2 = "pusher2"
ARM = "arm"
VACUUM = "vacuum"


# =============================================================================
# STATE
# =============================================================================

last_gpio_values: Dict[str, int] | None = None
last_published_state: Dict[str, Any] | None = None

horizontal: int | None = None
diagonal: int | None = None
running: bool = False

last_gpio_seen_ts: float | None = None
last_pusher_seen_ts: float | None = None

health_main: bool = True
health_gpio: bool = True
health_pusher: bool = True

# When pusher2 is released into the measurement zone, we start a deadline
# expecting sensor3 to rise. If it doesn't, the box went missing.
box_expected_by_ts: float | None = None


# =============================================================================
# HELPERS
# =============================================================================


def rising_edge(prev: int, curr: int) -> bool:
    return prev == 0 and curr == 1


def falling_edge(prev: int, curr: int) -> bool:
    return prev == 1 and curr == 0


def measure_horizontal() -> int:
    value = random.randint(10, 100)
    log.info("Measured horizontal = %d", value)
    return value


def measure_diagonal() -> int:
    value = random.randint(10, 100)
    log.info("Measured diagonal = %d", value)
    return value


async def gpio_set(sock: zmq.asyncio.Socket, pin: str, value: bool) -> None:
    log.info("GPIO SET: %s -> %s", pin, value)
    await sock.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = await sock.recv_json()
    if reply.get("ok") is not True:
        raise RuntimeError(f"GPIO set failed: {reply}")


# =============================================================================
# MQTT STATE PUBLISHING
# =============================================================================


async def publish_state_if_changed(
    mqtt: aiomqtt.Client,
    gpio_values: Dict[str, int],
) -> None:
    global last_published_state

    compare_state = {
        "running": running,
        "gpio": gpio_values,
        "health": {
            "main": health_main,
            "gpio": health_gpio,
            "pusher": health_pusher,
        },
    }

    if compare_state == last_published_state:
        return

    publish_state = {
        "ts_ms": int(time.time() * 1000),
        **compare_state,
    }

    await mqtt.publish(
        MQTT_TOPIC_STATE,
        json.dumps(publish_state, separators=(",", ":")),
        qos=0,
        retain=True,
    )

    last_published_state = compare_state


# =============================================================================
# WATCHDOG
# =============================================================================


async def health_watchdog(mqtt: aiomqtt.Client) -> None:
    global health_gpio, health_pusher, health_main

    while True:
        await asyncio.sleep(0.5)
        now = time.monotonic()

        if last_gpio_seen_ts is not None:
            if now - last_gpio_seen_ts > HEALTH_TIMEOUT_SEC:
                health_gpio = False

        if last_pusher_seen_ts is not None:
            if now - last_pusher_seen_ts > HEALTH_TIMEOUT_SEC:
                health_pusher = False

        if not health_gpio or not health_pusher:
            health_main = False
            log.critical("Health failure detected, shutting down")
            await publish_state_if_changed(mqtt, last_gpio_values or {})
            sys.exit(1)


async def box_missing_watchdog(
    mqtt: aiomqtt.Client,
    gpio_req: zmq.asyncio.Socket,
) -> None:
    """
    If pusher2 has been released into the measurement zone and sensor3 does not
    go high in time, assume the box is missing. Safely stop the cycle:

    - ARM OFF
    - running = False
    - clear expectation
    """
    global box_expected_by_ts, running

    while True:
        await asyncio.sleep(0.1)
        if box_expected_by_ts is None:
            continue

        now = time.monotonic()
        if now <= box_expected_by_ts:
            continue

        log.error(
            "Box missing: sensor3 did not rise within %.1fs. "
            "Stopping cycle.",
            BOX_MISSING_TIMEOUT_SEC,
        )

        # Safe stop
        await gpio_set(gpio_req, ARM, False)
        await asyncio.sleep(RUNNING_RESET_DELAY_SEC)
        running = False
        box_expected_by_ts = None

        await publish_state_if_changed(mqtt, last_gpio_values or {})
        # Continue running service; this is not a fatal crash.


# =============================================================================
# GPIO LISTENER
# =============================================================================


async def gpio_listener(
    ctx: zmq.asyncio.Context,
    mqtt: aiomqtt.Client,
) -> None:
    global last_gpio_values, diagonal, running, last_gpio_seen_ts
    global box_expected_by_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(GPIO_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, GPIO_STATUS_TOPIC)

    req = ctx.socket(zmq.REQ)
    req.connect(GPIO_REP_ADDR)

    log.info("Subscribed to GPIO status")

    # Spawn missing-box watchdog once we have the req socket
    watchdog_task = asyncio.create_task(
        box_missing_watchdog(mqtt, req),
        name="box_missing_watchdog",
    )

    try:
        while True:
            _, raw = await sub.recv_multipart()
            last_gpio_seen_ts = time.monotonic()

            msg = json.loads(raw.decode("utf-8"))
            gpio_values = {
                pin: int(data["value"]) for pin, data in msg["pins"].items()
            }

            s2 = gpio_values[SENSOR2]
            s3 = gpio_values[SENSOR3]

            if last_gpio_values is None:
                last_gpio_values = gpio_values.copy()
                await publish_state_if_changed(mqtt, gpio_values)
                continue

            # -----------------------------------------------------------------
            # sensor2 ↑ -> start / queue logic
            # -----------------------------------------------------------------
            if rising_edge(last_gpio_values[SENSOR2], s2):
                log.info("EVENT: sensor2 ↑")
                await start_measurement_sequence(req)

            # -----------------------------------------------------------------
            # sensor3 ↑ -> diagonal station reached (box is present)
            # Clear missing-box expectation for current cycle.
            # -----------------------------------------------------------------
            if rising_edge(last_gpio_values[SENSOR3], s3):
                log.info("EVENT: sensor3 ↑")
                box_expected_by_ts = None

                diagonal = measure_diagonal()

                if horizontal is None:
                    log.warning("No horizontal recorded")
                    await asyncio.sleep(POST_MEASUREMENT_DELAY_SEC)
                    await gpio_set(req, ARM, False)

                elif diagonal > horizontal:
                    await gpio_set(req, VACUUM, True)
                    await gpio_set(req, ARM, False)
                    await asyncio.sleep(VACUUM_HOLD_DELAY_SEC)
                    await gpio_set(req, VACUUM, False)

                else:
                    await asyncio.sleep(POST_MEASUREMENT_DELAY_SEC)
                    await gpio_set(req, ARM, False)

            # -----------------------------------------------------------------
            # sensor3 ↓ -> box leaves system
            # -----------------------------------------------------------------
            if falling_edge(last_gpio_values[SENSOR3], s3):
                log.info("EVENT: sensor3 ↓")
                await asyncio.sleep(RUNNING_RESET_DELAY_SEC)
                running = False
                box_expected_by_ts = None

                # Queued box at pusher2
                if s2 == 1:
                    await start_measurement_sequence(req)

            last_gpio_values = gpio_values.copy()
            await publish_state_if_changed(mqtt, gpio_values)

    finally:
        watchdog_task.cancel()
        await asyncio.gather(watchdog_task, return_exceptions=True)


# =============================================================================
# PUSHER HEARTBEAT LISTENER
# =============================================================================


async def pusher_listener(ctx: zmq.asyncio.Context) -> None:
    global last_pusher_seen_ts

    sub = ctx.socket(zmq.SUB)
    sub.connect(PUSHER_PUB_ADDR)
    sub.setsockopt(zmq.SUBSCRIBE, PUSHER_HEARTBEAT_TOPIC)

    while True:
        await sub.recv_multipart()
        last_pusher_seen_ts = time.monotonic()


# =============================================================================
# MEASUREMENT SEQUENCE
# =============================================================================


async def start_measurement_sequence(req: zmq.asyncio.Socket) -> None:
    """
    Sequence:

    1) Stop box at pusher2
    2) If already running, do nothing else (box is queued)
    3) Else:
       - running = True
       - arm ON, settle
       - pusher2 OFF (release into measurement zone)
       - measure horizontal
       - start missing-box deadline waiting for sensor3 ↑
    """
    global running, horizontal, box_expected_by_ts

    await gpio_set(req, PUSHER2, True)

    if running:
        return

    running = True
    await gpio_set(req, ARM, True)
    await asyncio.sleep(ARM_SETTLE_DELAY_SEC)
    await gpio_set(req, PUSHER2, False)

    horizontal = measure_horizontal()

    box_expected_by_ts = time.monotonic() + BOX_MISSING_TIMEOUT_SEC


# =============================================================================
# ENTRY
# =============================================================================


async def _amain() -> None:
    ctx = zmq.asyncio.Context.instance()

    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as mqtt:
        await asyncio.gather(
            gpio_listener(ctx, mqtt),
            pusher_listener(ctx),
            health_watchdog(mqtt),
        )


def main() -> None:
    try:
        asyncio.run(_amain())
    except KeyboardInterrupt:
        log.critical("Interrupted")
        sys.exit(1)
    except Exception:
        log.critical("Fatal crash", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
