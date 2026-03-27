#!/usr/bin/env python3
"""Cumulocity Modbus device operation handler"""
import logging
from dataclasses import dataclass
import json
import requests
import toml

from .context import Context

logger = logging.getLogger("c8y_CanDevice")
logging.basicConfig(
    filename="/var/log/tedge/c8y_CanDevice.log",
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)


@dataclass
class CanDevice:
    """Can device details"""

    child_name: str
    device_id: str
    mapping_path: str


def update_or_create_device_mapping(target: CanDevice, mapping, new_mapping):
    """Update or create device mapping"""
    devices = mapping.setdefault("device", [])
    for i, device in enumerate(devices):
        if device.get("name") == target.child_name:
            devices[i] = get_device_from_mapping(target, new_mapping)
            return
    devices.append(get_device_from_mapping(target, new_mapping))


def get_device_from_mapping(target: CanDevice, mapping):
    """Get a device from a given mapping definition"""
    device = {"name": target.child_name, "registers": mapping["c8y_Registers"]}
    return device


def parse_arguments(arguments) -> CanDevice:
    """Parse operation arguments"""
    data = json.loads(arguments[0])
    return CanDevice(
        child_name=data["name"],
        device_id=data["id"],
        mapping_path=data["type"],
    )


def run(arguments, context: Context):
    """main"""
    loglevel = context.base_config["can"]["loglevel"] or "INFO"
    logger.setLevel(getattr(logging, loglevel.upper(), logging.INFO))
    logger.info("New c8y_CanDevice operation")
    # Check and store arguments
    if len(arguments) != 1:
        raise ValueError("Expected 1 argument. Got " + str(len(arguments)) + ".")
    config_path = context.config_dir / "devices.toml"
    target = parse_arguments(arguments)

    # Update external id of child device
    logger.debug("Create external id for child device %s", target.device_id)
    url = f"{context.c8y_proxy}/identity/globalIds/{target.device_id}/externalIds"
    data = {
        "externalId": f"{context.device_id}:device:{target.child_name}",
        "type": "c8y_Serial",
    }
    response = requests.post(url, json=data, timeout=60)
    if response.status_code != 201:
        raise ValueError(
            f"Error creating external id for child device with id {target.device_id}. "
            f"Got response {response.status_code} from {url}. Expected 201."
        )
    logger.info(
        "Created external id for child device with id %s to %s",
        target.device_id,
        data["externalId"],
    )

    # Get the mapping json via rest
    url = f"{context.c8y_proxy}{target.mapping_path}"
    logger.debug("Getting mapping json from %s", url)
    response = requests.get(url, timeout=60)
    logger.info("Got mapping json from %s with response %d", url, response.status_code)
    if response.status_code != 200:
        raise ValueError(
            f"Error getting mapping at {target.mapping_path}. "
            f"Got response {response.status_code} from {url}. Expected 200."
        )
    new_mapping = response.json()

    # Read the mapping toml from pathToConfig
    logger.debug("Reading mapping toml from %s", config_path)
    mapping = toml.load(config_path)
    logger.info("Read mapping toml from %s", config_path)

    # Update or create device data for the device with the same childName
    logger.debug(
        "Updating or creating device data for device with childName %s",
        target.child_name,
    )
    update_or_create_device_mapping(
        target,
        mapping,
        new_mapping,
    )

    logger.debug("Created mapping toml: %s", mapping)

    # Store the mapping toml:
    logger.debug("Storing mapping toml at %s", config_path)

    toml_str = toml.dumps(mapping)
    with open(config_path, "w", encoding="utf8") as file:
        file.write(toml_str)
    logger.info("Stored mapping toml at %s", config_path)
