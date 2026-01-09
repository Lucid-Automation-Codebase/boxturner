#!/usr/bin/env python3
"""
Comprehensive integration test for pusher microservice.

Tests:
- Heartbeat presence (service liveness)
- GPIO status visibility
- All edge combinations of sensor1 / sensor2
- Correct pusher1 ON/OFF behavior
- No false positives
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
STATE_PROPAGATION_SEC = 1.0

SENSOR1 = "sensor1"
SENSOR2 = "sensor2"
PUSHER1 = "pusher1"


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
# WAIT HELPERS
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


def wait_for_pusher(expected: int) -> None:
    deadline = time.time() + STATE_PROPAGATION_SEC
    while time.time() < deadline:
        state = get_gpio_state()
        if state[PUSHER1] == expected:
            return
    raise AssertionError(f"Pusher1 did not reach expected state {expected}")


def wait_for_baseline() -> None:
    deadline = time.time() + 2.0
    while time.time() < deadline:
        state = get_gpio_state()
        if state[SENSOR1] == 0 and state[SENSOR2] == 0 and state[PUSHER1] == 0:
            return
    raise AssertionError("Failed to reach GPIO baseline")


# =============================================================================
# TEST MATRIX
# =============================================================================


def test_sensor_combinations() -> None:
    """
    Truth table (edge-based):

    sensor1 ↑ AND sensor2 == 1  -> pusher1 ON
    sensor2 ↓ AND pusher1 == 1 -> pusher1 OFF
    """

    print("Running sensor edge tests")

    # -------------------------------------------------------------------------
    # Establish known baseline BEFORE edges
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    gpio_set(SENSOR2, False)
    gpio_set(PUSHER1, False)

    wait_for_baseline()
    time.sleep(0.2)

    # -------------------------------------------------------------------------
    # Case 1:
    # sensor1 ↑ while sensor2 == 0 → NO ACTION
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    time.sleep(0.2)
    wait_for_pusher(0)
    print("✓ sensor1 ↑ with sensor2 OFF does nothing")

    # -------------------------------------------------------------------------
    # Case 2:
    # sensor2 ON, sensor1 already ON → NO EDGE → NO ACTION
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    time.sleep(0.2)
    wait_for_pusher(0)
    print("✓ sensor2 ON without sensor1 edge does nothing")

    # -------------------------------------------------------------------------
    # Case 3:
    # sensor1 ↓ then ↑ while sensor2 ON → PUSHER ON
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    time.sleep(0.1)
    gpio_set(SENSOR1, True)
    wait_for_pusher(1)
    print("✓ sensor1 ↑ while sensor2 ON turns pusher ON")

    # -------------------------------------------------------------------------
    # Case 4:
    # sensor1 ↓ while sensor2 ON → NO EFFECT
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    time.sleep(0.2)
    wait_for_pusher(1)
    print("✓ sensor1 ↓ does not affect pusher")

    # -------------------------------------------------------------------------
    # Case 5:
    # sensor2 ↓ while pusher ON → PUSHER OFF
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, False)
    wait_for_pusher(0)
    print("✓ sensor2 ↓ turns pusher OFF")

    # -------------------------------------------------------------------------
    # Case 6:
    # sensor2 ↑ again → NO EFFECT
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    time.sleep(0.2)
    wait_for_pusher(0)
    print("✓ sensor2 ↑ alone does nothing")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    try:
        print("Waiting for pusher heartbeat...")
        wait_for_heartbeat()

        test_sensor_combinations()

    except Exception as e:
        print("✗ TEST FAILED:", e)
        sys.exit(1)

    print("\n✓ ALL PUSHER TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
