#!/usr/bin/env python3
"""
Integration tests for main sequencer

Adjusted for pusher1 pulse+hold behavior.

Scenarios:
- Scenario 1: single box, repeated
- Scenario 2: queued boxes with upstream interference
- Scenario 3: missing box timeout
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Dict, Callable

import zmq
import paho.mqtt.client as mqtt


# =============================================================================
# CONFIG
# =============================================================================

GPIO_REP_ADDR = "tcp://127.0.0.1:5557"

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC_STATE = "lucid/boxturner/state"

RECV_TIMEOUT_MS = 2000

# Pins
SENSOR1 = "sensor1"
SENSOR2 = "sensor2"
SENSOR3 = "sensor3"

PUSHER1 = "pusher1"
PUSHER2 = "pusher2"
ARM = "arm"
VACUUM = "vacuum"

ALL_PINS = [
    SENSOR1,
    SENSOR2,
    SENSOR3,
    PUSHER1,
    PUSHER2,
    ARM,
    VACUUM,
]

# Timing
STATE_TIMEOUT_SEC = 5.0
BOX_EXIT_SEC = 1.2

# --- must match pusher service ---
PUSHER_ON_SEC = 0.50
PUSHER_OFF_SEC = 0.40
PUSHER_PULSES = 3

TOTAL_PULSE_TIME = (
    PUSHER_PULSES * PUSHER_ON_SEC + (PUSHER_PULSES - 1) * PUSHER_OFF_SEC
)


# =============================================================================
# ZMQ HELPERS
# =============================================================================

ctx = zmq.Context.instance()


def make_req() -> zmq.Socket:
    s = ctx.socket(zmq.REQ)
    s.connect(GPIO_REP_ADDR)
    s.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return s


def gpio_set(pin: str, value: bool) -> None:
    req = make_req()
    req.send_json({"cmd": "set", "pin": pin, "value": value})
    reply = req.recv_json()
    assert reply["ok"] is True, reply


def reset_all_pins() -> None:
    for pin in ALL_PINS:
        gpio_set(pin, False)
    time.sleep(0.3)


# =============================================================================
# MQTT STATE TRACKING
# =============================================================================

mqtt_state: Dict[str, Any] | None = None


def on_mqtt_message(_client, _userdata, msg) -> None:
    global mqtt_state
    mqtt_state = json.loads(msg.payload.decode("utf-8"))


def wait_for_state(
    predicate: Callable[[Dict[str, Any]], bool],
    timeout: float = STATE_TIMEOUT_SEC,
) -> Dict[str, Any]:
    deadline = time.time() + timeout
    while time.time() < deadline:
        if mqtt_state and predicate(mqtt_state):
            return mqtt_state
        time.sleep(0.05)
    raise AssertionError("Timed out waiting for state condition")


# =============================================================================
# SCENARIO 1
# =============================================================================


def scenario_1() -> None:
    print("\n=== Scenario 1: single box + sealer path ===")

    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)
    print("✓ baseline idle")

    # --------------------------------------------------------------
    # sensor1 upstream noise (pulses then releases)
    # --------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (upstream noise)")
    time.sleep(TOTAL_PULSE_TIME + 0.2)

    state = mqtt_state
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    print("✓ pusher1 pulsed and released (no downstream hold)")

    gpio_set(SENSOR1, False)
    time.sleep(0.5)

    # --------------------------------------------------------------
    # sensor2 enters → pusher2 actuates
    # --------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑")

    wait_for_state(lambda s: s["running"] is True)
    print("✓ running == True")

    gpio_set(SENSOR2, False)
    print("✓ sensor2 ↓")
    time.sleep(2.0)

    # --------------------------------------------------------------
    # sensor3 → arm open
    # --------------------------------------------------------------
    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑")
    time.sleep(1.0)

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    print("✓ arm opened")

    gpio_set(SENSOR3, False)
    time.sleep(1.0)

    # --------------------------------------------------------------
    # sealer path (accept)
    # --------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑")
    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑")
    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)

    gpio_set("sensor5", False)
    time.sleep(1.0)

    wait_for_state(lambda s: s["running"] is False)

    print("✓ Scenario 1 PASSED")


# =============================================================================
# SCENARIO 2
# =============================================================================


def scenario_2() -> None:
    print("\n=== Scenario 2: queued boxes with upstream interference ===")

    # ------------------------------------------------------------------
    # Baseline
    # ------------------------------------------------------------------
    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)
    print("✓ baseline idle")

    # ------------------------------------------------------------------
    # Phase A: box1 enters pusher2 FIRST (sensor2 ON)
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑ (box1 reaches pusher2)")

    wait_for_state(lambda s: s["running"] is True, timeout=5)
    print("✓ running == True (box1 held at pusher2)")

    # ------------------------------------------------------------------
    # Phase B: box2 arrives upstream during processing (sensor1 ↑)
    # With sensor2 ON, pusher1 must pulse then HOLD.
    # ------------------------------------------------------------------
    gpio_set(SENSOR1, True)
    print("✓ sensor1 ↑ (box2 upstream)")

    time.sleep(TOTAL_PULSE_TIME + 0.2)

    wait_for_state(lambda s: s["gpio"][PUSHER1] == 1)
    print("✓ pusher1 holding box2 after pulse sequence")

    # ------------------------------------------------------------------
    # Phase C: box1 released into measurement (sensor2 ↓)
    # This also clears downstream, so pusher1 should release.
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, False)
    print("✓ sensor2 ↓ (box1 released)")

    wait_for_state(lambda s: s["gpio"][PUSHER1] == 0, timeout=5)
    print("✓ pusher1 released box2 on sensor2 ↓")

    # box2 moves down; clear sensor1 as it leaves upstream sensor
    gpio_set(SENSOR1, False)
    time.sleep(0.5)

    # ------------------------------------------------------------------
    # Phase D: box2 queues at pusher2
    # ------------------------------------------------------------------
    gpio_set(SENSOR2, True)
    print("✓ sensor2 ↑ (box2 reaches pusher2)")

    wait_for_state(lambda s: s["gpio"][PUSHER2] == 1)
    print("✓ pusher2 holding box2")

    # ------------------------------------------------------------------
    # Phase E: box1 diagonal measurement
    # ------------------------------------------------------------------
    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑ (box1 diagonal)")

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    gpio_set(SENSOR3, False)
    print("✓ box1 exited diagonal")

    # ------------------------------------------------------------------
    # Phase F: box1 sealer path
    # ------------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑ (box1 at sealer)")

    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)
    print("✓ pusher4 ON (box1)")

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑ (box1 sealed)")

    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)
    print("✓ pusher3 ON (box1)")

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Phase G: box2 released and measured
    # ------------------------------------------------------------------
    wait_for_state(lambda s: s["gpio"][PUSHER2] == 0)
    print("✓ pusher2 released box2")

    gpio_set(SENSOR2, False)
    time.sleep(0.5)

    gpio_set(SENSOR3, True)
    print("✓ sensor3 ↑ (box2 diagonal)")

    wait_for_state(lambda s: s["gpio"][ARM] == 0)
    gpio_set(SENSOR3, False)
    print("✓ box2 exited diagonal")

    # ------------------------------------------------------------------
    # Phase H: box2 sealer path
    # ------------------------------------------------------------------
    gpio_set("sensor4", True)
    print("✓ sensor4 ↑ (box2 at sealer)")

    wait_for_state(lambda s: s["gpio"].get("pusher4", 0) == 1)
    print("✓ pusher4 ON (box2)")

    gpio_set("sensor4", False)
    time.sleep(0.5)

    gpio_set("sensor5", True)
    print("✓ sensor5 ↑ (box2 sealed)")

    wait_for_state(lambda s: s["gpio"].get("pusher3", 0) == 1)
    print("✓ pusher3 ON (box2)")

    gpio_set("sensor5", False)
    time.sleep(1.0)

    # ------------------------------------------------------------------
    # Final state
    # ------------------------------------------------------------------
    wait_for_state(lambda s: s["running"] is False)

    state = mqtt_state
    assert state["gpio"][PUSHER1] == 0
    assert state["gpio"][PUSHER2] == 0
    assert state["gpio"].get("pusher3", 0) == 0
    assert state["gpio"].get("pusher4", 0) == 0

    print("✓ system returned to idle")
    print("✓ Scenario 2 PASSED")


# =============================================================================
# SCENARIO 3
# =============================================================================


def scenario_3() -> None:
    print("\n=== Scenario 3: missing box timeout ===")

    reset_all_pins()
    wait_for_state(lambda s: s["running"] is False)

    gpio_set(SENSOR1, True)
    time.sleep(TOTAL_PULSE_TIME + 0.2)
    gpio_set(SENSOR1, False)

    gpio_set(SENSOR2, True)
    wait_for_state(lambda s: s["running"] is True)

    gpio_set(SENSOR2, False)
    time.sleep(2.0)

    print("⏳ waiting for BOX_MISSING_TIMEOUT...")
    wait_for_state(lambda s: s["gpio"][ARM] == 0, timeout=11.0)

    gpio_set("sensor4", True)
    time.sleep(1.0)

    state = mqtt_state
    assert state["gpio"].get("pusher4", 0) == 0
    assert state["gpio"].get("pusher3", 0) == 0

    gpio_set("sensor4", False)
    gpio_set("sensor5", True)
    time.sleep(0.5)
    gpio_set("sensor5", False)

    wait_for_state(lambda s: s["running"] is False)
    print("✓ Scenario 3 PASSED")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_mqtt_message
    client.connect(MQTT_HOST, MQTT_PORT, 60)
    client.subscribe(MQTT_TOPIC_STATE)
    client.loop_start()

    try:
        wait_for_state(lambda s: True)

        scenario_1()
        scenario_2()
        scenario_3()

    except Exception as e:
        print("✗ TEST FAILED:", e)
        sys.exit(1)

    finally:
        client.loop_stop()

    print("\n✓ ALL TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
