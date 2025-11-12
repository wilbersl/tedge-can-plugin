#!/usr/bin/env python3
"""Cumulocity CanConfiguration operation handler"""
import json
import logging
import toml
from paho.mqtt.publish import single as mqtt_publish

from .context import Context

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)


def run(arguments, context: Context):
    """Run c8y_CanConfiguration operation handler"""
    if len(arguments) != 1:
        raise ValueError(f"Expected 1 argument. Got {len(arguments)}")
    # Get device configuration
    can_config = context.base_config
    loglevel = can_config["can"]["loglevel"] or "INFO"
    logger.setLevel(getattr(logging, loglevel.upper(), logging.INFO))
    logger.info("New c8y_CanConfiguration operation")
    logger.debug("Current configuration: %s", can_config)
    data = json.loads(arguments[0])
    transmit_rate = data["transmitRate"]
    baud_rate = data["baudRate"]
    logger.debug("transmitRate: %d, baudRate: %d", transmit_rate, baud_rate)

    # Update configuration
    can_config["can"]["transmitrate"] = transmit_rate
    can_config["can"]["baudrate"] = baud_rate

    # Save to file
    logger.info("Saving new can configuration to %s", context.base_config_path)
    with open(context.base_config_path, "w", encoding="utf8") as f:
        toml.dump(can_config, f)

    # Update managedObject
    logger.debug("Updating managedObject with new configuration")

    config = {
        "transmitRate": transmit_rate,
        "baudRate": baud_rate,
    }
    # pylint: disable=duplicate-code
    mqtt_publish(
        topic="te/device/main///twin/c8y_CanConfiguration",
        payload=json.dumps(config),
        qos=1,
        retain=True,
        hostname=context.broker,
        port=context.port,
        client_id="c8y_CanConfiguration-operation-client",
    )
