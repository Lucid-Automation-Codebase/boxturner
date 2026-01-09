#!/usr/bin/env python3
"""
Async GPIO microservice (fail-fast)

- gpiozero + pin factory
- ZMQ PUB: GPIO status
- ZMQ REP: control commands
- Logs all commands and all input/output changes
- Any error (including Ctrl-C) is logged and the process exits
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
from gpiozero import DigitalInputDevice, DigitalOutputDevice
from gpiozero.pins.mock import MockFactory

# from gpiozero.pins.pigpio import PiGPIOFactory


# =============================================================================
# LOGGING
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] gpio: %(message)s",
)
log = logging.getLogger("gpio")


# =============================================================================
# CONSTANTS
# =============================================================================

DEVICE_ID = "gpio"

ZMQ_PUB_BIND = "tcp://0.0.0.0:5556"
ZMQ_REP_BIND = "tcp://0.0.0.0:5557"
STATUS_TOPIC = b"gpio.status"
PUBLISH_HZ = 10.0

PIN_FACTORY = MockFactory()
# PIN_FACTORY = PiGPIOFactory()

OUTPUT_PINS = {
    "pusher1": {"pin": 17, "active_high": True, "initial": False},
    "pusher2": {"pin": 18, "active_high": True, "initial": False},
    "pusher3": {"pin": 23, "active_high": True, "initial": False},
    "pusher4": {"pin": 24, "active_high": True, "initial": False},
    "vacuum": {"pin": 27, "active_high": True, "initial": False},
    "arm": {"pin": 22, "active_high": True, "initial": False},
}

INPUT_PINS = {
    "sensor1": {"pin": 5, "pull_up": False},
    "sensor2": {"pin": 6, "pull_up": False},
    "sensor3": {"pin": 13, "pull_up": False},
    "sensor4": {"pin": 21, "pull_up": False},
    "sensor5": {"pin": 19, "pull_up": False},
}


# =============================================================================
# GPIO INIT
# =============================================================================

log.info("Initializing GPIO")

outputs: Dict[str, DigitalOutputDevice] = {}
inputs: Dict[str, DigitalInputDevice] = {}

for name, cfg in OUTPUT_PINS.items():
    outputs[name] = DigitalOutputDevice(
        pin=cfg["pin"],
        active_high=cfg["active_high"],
        initial_value=cfg["initial"],
        pin_factory=PIN_FACTORY,
    )
    log.info("Output initialized: %s=%d", name, int(outputs[name].value))

for name, cfg in INPUT_PINS.items():
    inputs[name] = DigitalInputDevice(
        pin=cfg["pin"],
        pull_up=cfg["pull_up"],
        pin_factory=PIN_FACTORY,
    )
    log.info("Input initialized: %s=%d", name, int(inputs[name].value))


def read_all_pins() -> Dict[str, Dict[str, int]]:
    state: Dict[str, Dict[str, int]] = {}

    for name, dev in outputs.items():
        state[name] = {"direction": "out", "value": int(dev.value)}

    for name, dev in inputs.items():
        state[name] = {"direction": "in", "value": int(dev.value)}

    return state


# =============================================================================
# ASYNC TASKS
# =============================================================================


async def status_publisher(ctx: zmq.asyncio.Context) -> None:
    log.info("Starting ZMQ PUB on %s", ZMQ_PUB_BIND)

    sock = ctx.socket(zmq.PUB)
    sock.bind(ZMQ_PUB_BIND)

    period = 1.0 / PUBLISH_HZ

    while True:
        payload = {
            "type": "gpio_status",
            "device_id": DEVICE_ID,
            "ts_ms": int(time.time() * 1000),
            "pins": read_all_pins(),
        }

        await sock.send_multipart(
            [
                STATUS_TOPIC,
                json.dumps(payload, separators=(",", ":")).encode("utf-8"),
            ]
        )

        await asyncio.sleep(period)


async def command_server(ctx: zmq.asyncio.Context) -> None:
    log.info("Starting ZMQ REP on %s", ZMQ_REP_BIND)

    sock = ctx.socket(zmq.REP)
    sock.bind(ZMQ_REP_BIND)

    while True:
        raw = await sock.recv()

        req = json.loads(raw.decode("utf-8"))

        cmd = req["cmd"]

        if cmd == "ping":
            log.info("PING")
            await sock.send_json({"ok": True})
            continue

        if cmd == "get_status":
            log.info("GET_STATUS")
            await sock.send_json({"ok": True, "pins": read_all_pins()})
            continue

        if cmd == "set":
            pin = req["pin"]
            value = req["value"]

            if not isinstance(value, bool):
                raise RuntimeError("Value must be boolean")

            # ---- OUTPUT ------------------------------------------------------
            if pin in outputs:
                old = int(outputs[pin].value)
                new = 1 if value else 0

                if old != new:
                    log.info(
                        "OUTPUT CHANGE: %s %d → %d",
                        pin,
                        old,
                        new,
                    )

                outputs[pin].value = new
                await sock.send_json({"ok": True, "pin": pin, "value": value})
                continue

            # ---- INPUT (MockFactory only) -----------------------------------
            if pin in inputs:
                mock_pin = getattr(inputs[pin].pin, "drive_high", None)
                if mock_pin is None:
                    raise RuntimeError(
                        f"Cannot drive input pin {pin} on real hardware"
                    )

                old = int(inputs[pin].value)
                new = 1 if value else 0

                if old != new:
                    log.info(
                        "INPUT DRIVE: %s %d → %d",
                        pin,
                        old,
                        new,
                    )

                if value:
                    inputs[pin].pin.drive_high()
                else:
                    inputs[pin].pin.drive_low()

                await sock.send_json({"ok": True, "pin": pin, "value": value})
                continue

            raise RuntimeError(f"Invalid pin: {pin}")

        raise RuntimeError(f"Unknown command: {cmd}")


# =============================================================================
# MAIN
# =============================================================================


async def _amain() -> None:
    log.info("GPIO service starting")

    ctx = zmq.asyncio.Context.instance()

    tasks = [
        asyncio.create_task(status_publisher(ctx), name="status_publisher"),
        asyncio.create_task(command_server(ctx), name="command_server"),
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
