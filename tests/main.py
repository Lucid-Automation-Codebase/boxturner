#!/usr/bin/env python3
"""
Integration tests for main sequencer

Aligned with FINAL pusher1 logic:
- sensor1 ↑:
    * sensor2 ON  -> pusher1 HOLD (eventually stable ON)
    * sensor2 OFF -> pusher1 PULSE while sensor1 ON, ending OFF
- sensor2 ↓ while holding -> switch to pulse (toggling allowed)
- sensor1 ↓ always ends pusher1 OFF (eventually)

Key test principles:
- Never assert immediate stability after mode switches.
- Use "eventual converge" for HOLD/OFF.
- Use "toggles within window" to confirm pulse mode.
"""

from __future__ import annotations

import json
import sys
import time
from typing import Any, Callable, Dict

import paho.mqtt.client as mqtt
import zmq


# =============================================================================
# CONFIG
# =============================================================================

GPIO_REP_ADDR = "tcp://127.0.0.1:5557"

MQTT_HOST = "127.0.0.1"
MQTT_PORT = 1883
MQTT_TOPIC_STATE = "lucid/boxturner/state"

RECV_TIMEOUT_MS = 2000
STATE_TIMEOUT_SEC = 5.0

# Pins
SENSOR1 = "sensor1"
SENSOR2 = "sensor2"
SENSOR3 = "sensor3"

PUSHER1 = "pusher1"
PUSHER2 = "pusher2"
ARM = "arm"
VACUUM = "vacuum"

SENSOR4 = "sensor4"
SENSOR5 = "sensor5"
PUSHER3 = "pusher3"
PUSHER4 = "pusher4"

ALL_PINS = [
    SENSOR1,
    SENSOR2,
    SENSOR3,
    SENSOR4,
    SENSOR5,
    PUSHER1,
    PUSHER2,
    PUSHER3,
    PUSHER4,
    ARM,
    VACUUM,
]

SAMPLE_INTERVAL_SEC = 0.05
TOGGLE_WINDOW_SEC = 0.9
HOLD_WINDOW_SEC = 0.4


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
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError("Timed out waiting for state condition")


def wait_for_gpio_level(
    pin: str, expected: int, timeout: float = STATE_TIMEOUT_SEC
) -> None:
    def pred(s: Dict[str, Any]) -> bool:
        return int(s["gpio"].get(pin, 0)) == expected

    wait_for_state(pred, timeout=timeout)


def assert_gpio_stable_level(pin: str, expected: int, window: float) -> None:
    deadline = time.time() + window
    while time.time() < deadline:
        s = mqtt_state
        if not s:
            time.sleep(SAMPLE_INTERVAL_SEC)
            continue
        val = int(s["gpio"].get(pin, 0))
        if val != expected:
            raise AssertionError(f"{pin} not stable at {expected}; saw {val}")
        time.sleep(SAMPLE_INTERVAL_SEC)


def assert_gpio_toggles(pin: str, window: float) -> None:
    seen: set[int] = set()
    deadline = time.time() + window
    while time.time() < deadline:
        s = mqtt_state
        if s:
            seen.add(int(s["gpio"].get(pin, 0)))
            if 0 in seen and 1 in seen:
                return
        time.sleep(SAMPLE_INTERVAL_SEC)
    raise AssertionError(f"{pin} did not toggle; seen={sorted(seen)}")


# =============================================================================
# SCENARIO 1
# =============================================================================


def scenario_1() -> None:
    print("\n=== Scenario 1: single box ===")

    reset_all_pins()
    wait_for_state(lambda s: s.get("running") is False)

    # sensor1 noise with sensor2 OFF -> pulse mode -> must end OFF after sensor1 ↓
    gpio_set(SENSOR1, True)
    assert_gpio_toggles(PUSHER1, TOGGLE_WINDOW_SEC)
    gpio_set(SENSOR1, False)
    wait_for_gpio_level(PUSHER1, 0)
    print("✓ upstream pulse ends released")

    # normal flow (box enters sensor2)
    gpio_set(SENSOR2, True)
    time.sleep(2)
    gpio_set(SENSOR2, False)
    wait_for_state(lambda s: s.get("running") is True)
    print("✓ running after sensor 2")

    gpio_set(SENSOR3, True)
    wait_for_gpio_level(ARM, 0)
    gpio_set(SENSOR3, False)

    gpio_set(SENSOR4, True)
    wait_for_gpio_level(PUSHER4, 1)
    gpio_set(SENSOR4, False)

    gpio_set(SENSOR5, True)
    wait_for_gpio_level(PUSHER3, 1)
    gpio_set(SENSOR5, False)

    wait_for_state(lambda s: s.get("running") is False)
    print("✓ Scenario 1 PASSED")


# =============================================================================
# SCENARIO 2
# =============================================================================


def scenario_2() -> None:
    print("\n=== Scenario 2: queued boxes with interference ===")

    reset_all_pins()
    wait_for_state(lambda s: s.get("running") is False)

    # --------------------------------------------------
    # Box 1 arrives at pusher2
    # --------------------------------------------------
    gpio_set(SENSOR2, True)
    wait_for_state(lambda s: s.get("running") is True)
    print("✓ box1 captured at pusher2")

    # --------------------------------------------------
    # Box 2 arrives upstream while box1 is held
    # --------------------------------------------------
    gpio_set(SENSOR1, True)
    wait_for_gpio_level(PUSHER1, 1)
    assert_gpio_stable_level(PUSHER1, 1, HOLD_WINDOW_SEC)
    print("✓ pusher1 holding box2")

    # --------------------------------------------------
    # Box 1 released → pusher1 downgraded to pulse
    # --------------------------------------------------
    gpio_set(SENSOR2, False)
    assert_gpio_toggles(PUSHER1, TOGGLE_WINDOW_SEC)
    print("✓ pusher1 downgraded HOLD → PULSE")

    # End pulse by clearing sensor1
    gpio_set(SENSOR1, False)
    wait_for_gpio_level(PUSHER1, 0)
    print("✓ pusher1 released box2")

    # --------------------------------------------------
    # Box 2 reaches pusher2
    # --------------------------------------------------
    gpio_set(SENSOR2, True)
    wait_for_gpio_level(PUSHER2, 1)
    print("✓ box2 captured at pusher2")

    # --------------------------------------------------
    # Box 1 diagonal
    # --------------------------------------------------
    gpio_set(SENSOR3, True)
    time.sleep(0.5)
    wait_for_gpio_level(ARM, 0)
    gpio_set(SENSOR3, False)
    print("✓ box1 diagonal complete")

    # --------------------------------------------------
    # Box 1 sealer
    # --------------------------------------------------
    gpio_set(SENSOR4, True)
    wait_for_gpio_level(PUSHER4, 1)
    gpio_set(SENSOR4, False)

    gpio_set(SENSOR5, True)
    wait_for_gpio_level(PUSHER3, 1)
    gpio_set(SENSOR5, False)
    print("✓ box1 sealed")

    # --------------------------------------------------
    # Box 2 released from pusher2
    # --------------------------------------------------
    wait_for_gpio_level(PUSHER2, 0)
    gpio_set(SENSOR2, False)
    print("✓ box2 released to diagonal")

    # --------------------------------------------------
    # Box 2 diagonal
    # --------------------------------------------------
    gpio_set(SENSOR3, True)
    wait_for_gpio_level(ARM, 0)
    gpio_set(SENSOR3, False)
    print("✓ box2 diagonal complete")

    # --------------------------------------------------
    # Box 2 sealer
    # --------------------------------------------------
    gpio_set(SENSOR4, True)
    wait_for_gpio_level(PUSHER4, 1)
    gpio_set(SENSOR4, False)

    gpio_set(SENSOR5, True)
    time.sleep(0.5)
    wait_for_gpio_level(PUSHER3, 1)
    gpio_set(SENSOR5, False)
    print("✓ box2 sealed")

    # --------------------------------------------------
    # Final idle
    # --------------------------------------------------
    wait_for_state(lambda s: s.get("running") is False)
    print("✓ Scenario 2 PASSED")


# =============================================================================
# SCENARIO 3
# =============================================================================


def scenario_3() -> None:
    print("\n=== Scenario 3: missing box timeout ===")

    reset_all_pins()
    wait_for_state(lambda s: s.get("running") is False)

    gpio_set(SENSOR2, True)
    time.sleep(2)
    wait_for_state(lambda s: s.get("running") is True)

    gpio_set(SENSOR2, False)

    wait_for_gpio_level(ARM, 0, timeout=11.0)

    gpio_set(SENSOR4, True)
    time.sleep(0.5)

    state = mqtt_state or {}
    gpio = state.get("gpio", {})
    assert int(gpio.get(PUSHER3, 0)) == 0
    assert int(gpio.get(PUSHER4, 0)) == 0

    gpio_set(SENSOR4, False)
    gpio_set(SENSOR5, True)
    time.sleep(0.5)
    gpio_set(SENSOR5, False)

    wait_for_state(lambda s: s.get("running") is False)
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

        # scenario_1()
        scenario_2()
        # scenario_3()

    except Exception as e:
        print("✗ TEST FAILED:", e)
        sys.exit(1)

    finally:
        client.loop_stop()

    print("\n✓ ALL TESTS PASSED")
    sys.exit(0)


if __name__ == "__main__":
    main()
