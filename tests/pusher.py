#!/usr/bin/env python3
"""
Comprehensive integration test for pusher microservice.

Verified contract:
- sensor1 ↑ :
    * if sensor2 ON -> pusher1 eventually HOLDs ON
    * else -> pusher1 pulses while sensor1 ON, ending OFF
- sensor2 ↓ :
    * if sensor1 ON and pusher1 ON -> switches to pulse mode
- sensor1 ↓ :
    * always results in pusher1 OFF (eventually)

IMPORTANT:
- Tests assert *eventual convergence*, not instantaneous stability.
- Transient OFF edges during task cancellation are allowed.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any

import zmq


# =============================================================================
# CONFIG
# =============================================================================

GPIO_PUB_ADDR = "tcp://127.0.0.1:5556"
GPIO_REP_ADDR = "tcp://127.0.0.1:5557"
GPIO_STATUS_TOPIC = b"gpio.status"

PUSHER_HEARTBEAT_ADDR = "tcp://127.0.0.1:5560"
PUSHER_HEARTBEAT_TOPIC = b"pusher.heartbeat"

RECV_TIMEOUT_MS = 2000
HEARTBEAT_TIMEOUT_SEC = 3.0
STATE_TIMEOUT_SEC = 2.0

SENSOR1 = "sensor1"
SENSOR2 = "sensor2"
PUSHER1 = "pusher1"

SAMPLE_INTERVAL_SEC = 0.05
TOGGLE_WINDOW_SEC = 0.9
HOLD_WINDOW_SEC = 0.4


# =============================================================================
# ZMQ HELPERS
# =============================================================================

ctx = zmq.Context.instance()


def make_sub(addr: str, topic: bytes) -> zmq.Socket:
    s = ctx.socket(zmq.SUB)
    s.connect(addr)
    s.setsockopt(zmq.SUBSCRIBE, topic)
    s.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return s


def make_req(addr: str) -> zmq.Socket:
    c = ctx.socket(zmq.REQ)
    c.connect(addr)
    c.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return c


def recv_multipart(sock: zmq.Socket) -> tuple[bytes, dict[str, Any]]:
    topic, msg = sock.recv_multipart()
    return topic, json.loads(msg.decode("utf-8"))


# =============================================================================
# STATE HELPERS
# =============================================================================


def wait_for_heartbeat() -> None:
    sub = make_sub(PUSHER_HEARTBEAT_ADDR, PUSHER_HEARTBEAT_TOPIC)
    deadline = time.time() + HEARTBEAT_TIMEOUT_SEC

    while time.time() < deadline:
        try:
            recv_multipart(sub)
            print("✓ Heartbeat detected")
            return
        except zmq.error.Again:
            pass

    raise AssertionError("No pusher heartbeat detected")


def get_gpio_state() -> dict[str, int]:
    sub = make_sub(GPIO_PUB_ADDR, GPIO_STATUS_TOPIC)
    _, msg = recv_multipart(sub)
    return {k: v["value"] for k, v in msg["pins"].items()}


def gpio_set(pin: str, value: bool) -> None:
    c = make_req(GPIO_REP_ADDR)
    c.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = c.recv_json()
    assert reply["ok"] is True


def wait_for_baseline() -> None:
    deadline = time.time() + 2.0
    while time.time() < deadline:
        s = get_gpio_state()
        if s[SENSOR1] == 0 and s[SENSOR2] == 0 and s[PUSHER1] == 0:
            return
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError("Failed to reach baseline")


def wait_for_eventual_off(timeout: float = STATE_TIMEOUT_SEC) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if get_gpio_state()[PUSHER1] == 0:
            return
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError("Pusher1 did not eventually settle OFF")


def wait_for_eventual_on(timeout: float = STATE_TIMEOUT_SEC) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if get_gpio_state()[PUSHER1] == 1:
            return
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError("Pusher1 did not eventually settle ON")


def assert_stable_level(expected: int, window_sec: float) -> None:
    deadline = time.time() + window_sec
    while time.time() < deadline:
        val = get_gpio_state()[PUSHER1]
        if val != expected:
            raise AssertionError(
                f"Pusher1 not stable at {expected}; saw {val}"
            )
        time.sleep(SAMPLE_INTERVAL_SEC)


def assert_toggles(window_sec: float) -> None:
    seen: set[int] = set()
    deadline = time.time() + window_sec
    while time.time() < deadline:
        seen.add(get_gpio_state()[PUSHER1])
        if 0 in seen and 1 in seen:
            return
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError(
        f"Pusher1 did not toggle within {window_sec}s; seen={sorted(seen)}"
    )


# =============================================================================
# TESTS
# =============================================================================


def test_sensor_logic() -> None:
    print("Running sensor logic tests")

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    gpio_set(SENSOR2, False)
    gpio_set(PUSHER1, False)
    wait_for_baseline()

    # ------------------------------------------------------------------
    # Case 1: sensor1 ↑ with sensor2 OFF -> pulse mode
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    assert_toggles(TOGGLE_WINDOW_SEC)
    print("✓ sensor1 ↑ with sensor2 OFF enters pulse mode")

    gpio_set(SENSOR1, False)
    wait_for_eventual_off()
    print("✓ sensor1 ↓ ends pulse, pusher OFF")

    # ------------------------------------------------------------------
    # Case 2: sensor1 ↑ with sensor2 ON -> HOLD
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    gpio_set(SENSOR1, True)

    # allow pulse cancellation / cleanup to complete
    wait_for_eventual_on()
    assert_stable_level(1, HOLD_WINDOW_SEC)
    print("✓ sensor1 ↑ with sensor2 ON converges to stable HOLD")

    # ------------------------------------------------------------------
    # Case 3: sensor2 ↓ while holding -> pulse mode
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, False)
    assert_toggles(TOGGLE_WINDOW_SEC)
    print("✓ sensor2 ↓ switches HOLD → PULSE")

    gpio_set(SENSOR1, False)
    wait_for_eventual_off()
    print("✓ sensor1 ↓ ends pulse after sensor2 ↓")

    # ------------------------------------------------------------------
    # Case 4: sensor2 ↑ alone does nothing
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    assert_stable_level(0, 0.3)
    print("✓ sensor2 ↑ alone does nothing (pusher remains OFF)")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    try:
        print("Waiting for pusher heartbeat...")
        wait_for_heartbeat()
        test_sensor_logic()
    except Exception as e:
        print("✗ TEST FAILED:", e)
        sys.exit(1)

    print("\n✓ ALL PUSHER TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
