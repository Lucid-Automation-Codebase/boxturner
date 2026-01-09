#!/usr/bin/env python3
"""
Client-side integration tests for running GPIO service.

Assumptions:
- gpio_service is already running
- MockFactory or real GPIO is irrelevant to this test
- Tests interact ONLY via ZMQ (black-box)
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

PUB_ADDR = "tcp://127.0.0.1:5556"
REP_ADDR = "tcp://127.0.0.1:5557"
STATUS_TOPIC = b"gpio.status"

RECV_TIMEOUT_MS = 2000
STATUS_WAIT_SEC = 1.5


# =============================================================================
# ZMQ HELPERS
# =============================================================================


def make_ctx() -> zmq.Context:
    return zmq.Context.instance()


def make_sub(ctx: zmq.Context) -> zmq.Socket:
    s = ctx.socket(zmq.SUB)
    s.connect(PUB_ADDR)
    s.setsockopt(zmq.SUBSCRIBE, STATUS_TOPIC)
    s.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return s


def make_req(ctx: zmq.Context) -> zmq.Socket:
    c = ctx.socket(zmq.REQ)
    c.connect(REP_ADDR)
    c.setsockopt(zmq.RCVTIMEO, RECV_TIMEOUT_MS)
    return c


def recv_status(sub: zmq.Socket) -> dict[str, Any]:
    topic, msg = sub.recv_multipart()
    assert topic == STATUS_TOPIC
    return json.loads(msg.decode("utf-8"))


# =============================================================================
# TESTS
# =============================================================================


def test_pub_status_stream() -> None:
    ctx = make_ctx()
    sub = make_sub(ctx)

    status = recv_status(sub)

    assert status["type"] == "gpio_status"
    assert "device_id" in status
    assert "pins" in status

    pins = status["pins"]
    for name in (
        "pusher1",
        "vacuum",
        "arm",
        "sensor1",
        "sensor2",
        "sensor3",
    ):
        assert name in pins
        assert pins[name]["value"] in (0, 1)

    print("✓ PUB status stream OK")


def test_rep_ping() -> None:
    ctx = make_ctx()
    c = make_req(ctx)

    c.send_json({"cmd": "ping"})
    reply = c.recv_json()

    assert reply == {"ok": True}

    print("✓ REP ping OK")


def test_rep_get_status() -> None:
    ctx = make_ctx()
    c = make_req(ctx)

    c.send_json({"cmd": "get_status"})
    reply = c.recv_json()

    assert reply["ok"] is True
    assert "pins" in reply
    assert "vacuum" in reply["pins"]

    print("✓ REP get_status OK")


def test_set_output_reflected_in_pub() -> None:
    ctx = make_ctx()
    c = make_req(ctx)
    sub = make_sub(ctx)

    # set vacuum ON
    c.send_json({"cmd": "set", "pin": "vacuum", "value": True})
    reply = c.recv_json()

    assert reply["ok"] is True
    assert reply["pin"] == "vacuum"
    assert reply["value"] is True

    # wait for PUB update
    deadline = time.time() + STATUS_WAIT_SEC
    while time.time() < deadline:
        status = recv_status(sub)
        if status["pins"]["vacuum"]["value"] == 1:
            print("✓ Output change reflected in PUB")
            return

    raise AssertionError("Vacuum ON not reflected in PUB")


def test_set_output_toggle() -> None:
    ctx = make_ctx()
    c = make_req(ctx)

    c.send_json({"cmd": "set", "pin": "vacuum", "value": False})
    reply = c.recv_json()

    assert reply["ok"] is True
    assert reply["value"] is False

    print("✓ Output toggle OFF OK")


def test_invalid_command_causes_disconnect() -> None:
    """
    Fail-fast behavior:
    - Service should crash
    - Client will see timeout / no reply
    """
    ctx = make_ctx()
    c = make_req(ctx)

    c.send_json({"cmd": "explode"})

    try:
        c.recv()
    except zmq.error.Again:
        print("✓ Invalid command caused service failure (timeout)")
        return

    raise AssertionError("Service did not fail on invalid command")


# =============================================================================
# MAIN
# =============================================================================


def main() -> None:
    tests = [
        test_pub_status_stream,
        test_rep_ping,
        test_rep_get_status,
        test_set_output_reflected_in_pub,
        test_set_output_toggle,
        test_invalid_command_causes_disconnect,
    ]

    failures = 0
    for t in tests:
        try:
            t()
        except Exception as e:
            failures += 1
            print(f"✗ {t.__name__} FAILED:", e)

    if failures:
        print(f"\n{failures} test(s) failed")
        sys.exit(1)

    print("\nAll tests passed")
    sys.exit(0)


if __name__ == "__main__":
    main()
