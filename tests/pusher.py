#!/usr/bin/env python3
"""
Comprehensive integration test for pusher microservice.

Updated contract:
- On sensor1 rising edge:
    * pusher pulses 3 times (ON/OFF)
    * after pulses:
        - sensor2 ON  -> pusher1 held ON
        - sensor2 OFF -> pusher1 OFF
- sensor2 falling edge releases held pusher
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

# --- must match pusher service ---
PUSHER_ON_SEC = 0.50
PUSHER_OFF_SEC = 0.40
PUSHER_PULSES = 3

TOTAL_PULSE_TIME = (
    PUSHER_PULSES * PUSHER_ON_SEC + (PUSHER_PULSES - 1) * PUSHER_OFF_SEC
)

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


def wait_for_final_pusher(expected: int) -> None:
    deadline = time.time() + STATE_PROPAGATION_SEC
    while time.time() < deadline:
        state = get_gpio_state()
        if state[PUSHER1] == expected:
            return
    raise AssertionError(
        f"Pusher1 did not settle to expected state {expected}"
    )


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
    print("Running sensor sequence tests")

    # -------------------------------------------------------------------------
    # Establish baseline
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    gpio_set(SENSOR2, False)
    gpio_set(PUSHER1, False)

    wait_for_baseline()
    time.sleep(0.2)

    # -------------------------------------------------------------------------
    # Case 1:
    # sensor1 ↑ while sensor2 OFF
    # pulses then releases
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    time.sleep(TOTAL_PULSE_TIME + 0.1)
    wait_for_final_pusher(0)
    print("✓ sensor1 ↑ with sensor2 OFF pulses then releases")

    # -------------------------------------------------------------------------
    # Case 2:
    # sensor2 ON alone does nothing
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    time.sleep(0.2)
    wait_for_final_pusher(0)
    print("✓ sensor2 ON alone does nothing")

    # -------------------------------------------------------------------------
    # Case 3:
    # sensor1 ↓ then ↑ while sensor2 ON
    # pulses then holds
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    time.sleep(0.1)
    gpio_set(SENSOR1, True)

    time.sleep(TOTAL_PULSE_TIME + 0.1)
    wait_for_final_pusher(1)
    print("✓ sensor1 ↑ with sensor2 ON pulses then holds")

    # -------------------------------------------------------------------------
    # Case 4:
    # sensor1 ↓ does not affect held pusher
    # -------------------------------------------------------------------------
    gpio_set(SENSOR1, False)
    time.sleep(0.2)
    wait_for_final_pusher(1)
    print("✓ sensor1 ↓ does not affect held pusher")

    # -------------------------------------------------------------------------
    # Case 5:
    # sensor2 ↓ releases held pusher
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, False)
    wait_for_final_pusher(0)
    print("✓ sensor2 ↓ releases held pusher")

    # -------------------------------------------------------------------------
    # Case 6:
    # sensor2 ↑ alone does nothing
    # -------------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    time.sleep(0.2)
    wait_for_final_pusher(0)
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
