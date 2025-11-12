#!/usr/bin/env python3
"""Can reader"""
import argparse
import json
import logging
import os.path
import sched
import sys
import threading
import time
import copy

import tomli
from paho.mqtt import client as mqtt_client
import can

from watchdog.events import FileSystemEventHandler, DirModifiedEvent, FileModifiedEvent
from watchdog.observers import Observer

from .banner import BANNER
from .mapper import MappedMessage, CanMapper


DEFAULT_FILE_DIR = "/etc/tedge/plugins/can"
BASE_CONFIG_NAME = "can.toml"
DEVICES_CONFIG_NAME = "devices.toml"

class CanBusReader:
    """
    CAN-Bus Reader, der kontinuierlich Nachrichten liest,
    ein Dictionary mit den neuesten Daten je CAN-ID pflegt
    und auch Nachrichten senden kann.
    """

    def __init__(self, channel="can0", bustype="socketcan", bitrate=500000):
        self.bus = can.interface.Bus(channel=channel, bustype=bustype, bitrate=bitrate)
        self.latest_messages: dict[int, dict[str, object]] = {}
        self.running = False
        self.thread: threading.Thread | None = None
        self._lock = threading.Lock()

    def start(self):
        """Startet den Reader-Thread"""
        if not self.running:
            self.running = True
            self.thread = threading.Thread(target=self._read_loop, daemon=True)
            self.thread.start()

    def stop(self):
        """Stoppt den Reader-Thread"""
        self.running = False
        if self.thread:
            self.thread.join()

    def _read_loop(self) -> None:
        """Endlos-Loop zum Lesen von CAN-Nachrichten"""
        while self.running:
            msg = self.bus.recv(timeout=1.0)
            if msg:
#                msg_dict = {
#                    "data": msg.data,
#                    "timestamp": msg.timestamp,
#                }
                with self._lock:
                    self.latest_messages[msg.arbitration_id] = msg

    def get_latest(self, can_id):
        """Gibt die letzte Nachricht einer CAN-ID zurück (oder None)"""
        with self._lock: # thread-safe read
            return self.latest_messages.get(can_id)

    def get_all_latest(self) -> dict[int, dict[str, object]]:
        """
        Gibt eine tiefe Kopie aller aktuell gespeicherten Nachrichten zurück.

        Returns:
            dict: Tiefenkopie von self.latest_messages
        """
        with self._lock:  # thread-safe read
            return copy.deepcopy(self.latest_messages)

    def send_message(self, can_id: int, data: bytes):
        """
        Sendet eine CAN-Nachricht.

        Args:
            can_id (int): Ziel-CAN-ID
            data (bytes): Payload (max. 8 Byte bei Standard-CAN)
        """
        msg = can.Message(arbitration_id=can_id, data=data, is_extended_id=False)
        try:
            self.bus.send(msg)
            return True
        except can.CanError as e:
            print(f"Sendefehler: {e}")
            return False

class CanPoll:
    """Can Poller"""

    class ConfigFileChangedHandler(FileSystemEventHandler):
        """Configuration file changed handler"""

        poller = None

        def __init__(self, poller):
            self.poller = poller

        def on_modified(self, event):
            """handler called when a file is modified"""
            if isinstance(event, DirModifiedEvent):
                return
            if isinstance(event, FileModifiedEvent) and event.event_type == "modified":
                filename = os.path.basename(event.src_path)
                if filename in [BASE_CONFIG_NAME, DEVICES_CONFIG_NAME]:
                    self.poller.reread_config()

    logger: logging.Logger
    tedge_client: mqtt_client.Client = None
    poll_scheduler = sched.scheduler(time.time, time.sleep)
    base_config = {}
    devices = []
    config_dir = "."
    canReader = CanBusReader()

    def __init__(self, config_dir=".", logfile=None):
        self.config_dir = config_dir
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)
        if logfile is not None:
            fh = logging.FileHandler(logfile)
            fh.setFormatter(
                logging.Formatter(
                    "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
                )
            )
            self.logger.addHandler(fh)
        self.print_banner()
        self.canReader.start()

    def reread_config(self):
        """Reread the configuration"""
        self.logger.info("file change detected, reading files")
        new_base_config = self.read_base_definition(
            f"{self.config_dir}/{BASE_CONFIG_NAME}"
        )
        restart_required = False
        if len(new_base_config) > 1 and new_base_config != self.base_config:
            restart_required = True
            self.base_config = new_base_config
        loglevel = self.base_config["can"]["loglevel"] or "INFO"
        self.logger.setLevel(getattr(logging, loglevel.upper(), logging.INFO))
        new_devices = self.read_device_definition(
            f"{self.config_dir}/{DEVICES_CONFIG_NAME}"
        )
        if (
            len(new_devices) >= 1
            and new_devices.get("device")
            and new_devices.get("device") is not None
            and new_devices.get("device") != self.devices
        ):
            restart_required = True
            self.devices = new_devices["device"]
        if restart_required:
            self.logger.info("config change detected, restart polling")
            if self.tedge_client is not None and self.tedge_client.is_connected():
                self.tedge_client.disconnect()
            self.tedge_client = self.connect_to_tedge()
            # If connected to tedge, register service, update config
            time.sleep(5)
            self.register_child_devices(self.devices)
            self.register_service()
            self.update_base_config_on_device(self.base_config)
            self.canReader.stop()
            self.canReader = CanBusReader(bitrate=self.base_config["can"].get("baudrate", 500000))
            self.canReader.start()
            for evt in self.poll_scheduler.queue:
                self.poll_scheduler.cancel(evt)
            self.poll_data()

    def watch_config_files(self, config_dir):
        """Start watching configuration files for changes"""
        event_handler = self.ConfigFileChangedHandler(self)
        observer = Observer()
        observer.schedule(event_handler, config_dir)
        observer.start()
        try:
            while True:
                time.sleep(5)
        except Exception as err:
            observer.stop()
            self.logger.error("File observer failed, %s", err, exc_info=True)

    def print_banner(self):
        """Print the application banner"""
        self.logger.info(BANNER)
        self.logger.info("Author:        Rina,Mario,Murat")
        self.logger.info("Date:          12th October 2022")
        self.logger.info(
            "Description:   "
            "A service that extracts data from a Modbus Server "
            "and sends it to a local thin-edge.io broker."
        )
        self.logger.info(
            "Documentation: Please refer to the c8y-documentation wiki to find service description"
        )

    def poll_data(self):
        """Poll Can data"""
        for device in self.devices:
            mapper = CanMapper(device)
            self.process_data(device, mapper)

    def process_data(self, device, mapper):
        """Read Can msgs out of data"""
        self.logger.debug("Processing data for device %s", device["name"])
        device_combine_measurements = device.get(
            "combinemeasurements",
            self.base_config["can"].get("combinemeasurements", False),
        )
        combined_measuerement = None
        canData = self.canReader.get_all_latest()
        if device.get("registers") is not None:
            for register_definition in device["registers"]:
                self.logger.debug("CanData: %s", canData)
                try:
                    msg_id = int(register_definition["number"],base=16)
                    result = canData.get(msg_id, None)
                    self.logger.debug("Read CAN data for ID %s: %s", msg_id, result)
                    if result is not None:
                        msgs, temp = mapper.map_register(
                            result, register_definition, device_combine_measurements
                        )
                        self.logger.debug("Mapped messages: %s", msgs)
                        self.logger.debug("Mapped messages: %s", temp)
                        if combined_measuerement is not None and temp is not None:
                            combined_measuerement.extend_data(temp)
                        elif temp is not None:
                            combined_measuerement = temp
                        for msg in msgs:
                            self.send_tedge_message(msg)
                except Exception as e:
                    self.logger.error("Failed to map register: %s", e)

            # send combined measurement if any
            try:
                if combined_measuerement is not None:
                    self.send_tedge_message(combined_measuerement)
            except Exception as e:
                self.logger.error("Failed to send combined measurement: %s", e)

        interval = device.get(
            "transmitrate", self.base_config["can"]["transmitrate"]
        )
        self.poll_scheduler.enter(
            interval,
            1,
            self.process_data,
            (device, mapper),
        )

    def read_base_definition(self, base_path):
        """Read base definition file"""
        if os.path.exists(base_path):
            with open(base_path, mode="rb") as file:
                return tomli.load(file)
        else:
            self.logger.error("Base config file %s not found", base_path)
            return {}

    def read_device_definition(self, device_path):
        """Read device definition file"""
        if os.path.exists(device_path):
            with open(device_path, mode="rb") as file:
                return tomli.load(file)
        else:
            self.logger.error("Device config file %s not found", device_path)
            return {}

    def start_polling(self):
        """Start watching the configuration files and start polling the Modbus server"""
        self.reread_config()
        file_watcher_thread = threading.Thread(
            target=self.watch_config_files, args=[self.config_dir]
        )
        file_watcher_thread.daemon = True
        file_watcher_thread.start()
        self.poll_scheduler.run()

    def send_tedge_message(
        self, msg: MappedMessage, retain: bool = False, qos: int = 0
    ):
        """Send a thin-edge.io message via MQTT"""
        self.logger.debug("sending message %s to topic %s", msg.data, msg.topic)
        self.tedge_client.publish(
            topic=msg.topic, payload=msg.data, retain=retain, qos=qos
        )

    def on_connect(
        self, client, userdata, flags, rc
    ):  # pylint: disable=unused-argument
        """Callback for when the client receives a CONNACK response from the server"""
        if rc == 0:
            self.logger.debug("Connected to MQTT broker successfully")
        else:
            self.logger.error("Failed to connect to MQTT broker, return code %d", rc)

    def on_message(self, client, userdata, msg):  # pylint: disable=unused-argument
        """Callback for when a PUBLISH message is received from the server"""
        try:
            topic = msg.topic
            payload = msg.payload.decode("utf-8")
            self.logger.debug("Received message on topic %s: %s", topic, payload)
            self._handle_subscribed_message(topic, payload)
        except Exception as e:
            self.logger.error("Error processing subscribed message: %s", e)

    def on_disconnect(self, client, userdata, rc):  # pylint: disable=unused-argument
        """Callback for when the client disconnects from the broker"""
        if rc != 0:
            self.logger.warning(
                "Unexpected disconnection from MQTT broker, return code %d", rc
            )
        else:
            self.logger.debug("Disconnected from MQTT broker")

    def connect_to_tedge(self):
        """Connect to the thin-edge.io MQTT broker and return a connected MQTT client"""
        while True:
            try:
                broker = self.base_config["thinedge"]["mqtthost"]
                port = self.base_config["thinedge"]["mqttport"]
                client_id = "can-client"
                client = mqtt_client.Client(client_id)

                # Set up callbacks
                client.on_connect = self.on_connect
                client.on_message = self.on_message
                client.on_disconnect = self.on_disconnect

                client.connect(broker, port)
                self.logger.debug("Connected to MQTT broker at %s:%d", broker, port)

                # Start the network loop to handle callbacks
                client.loop_start()

                return client
            except Exception as e:
                self.logger.error("Failed to connect to thin-edge.io: %s", e)
                time.sleep(5)

    def update_base_config_on_device(self, base_config):
        """Update the base configuration"""
        self.logger.debug("Update base config on device")
        topic = "te/device/main///twin/c8y_CanConfiguration"
        transmit_rate = base_config["can"].get("transmitinterval")
        config = {
            "transmitRate": transmit_rate
        }
        self.send_tedge_message(
            MappedMessage(json.dumps(config), topic), retain=True, qos=1
        )

    def register_service(self):
        """Register the service with thin-edge.io"""
        self.logger.debug("Register tedge service on device")
        topic = "te/device/main/service/tedge-can-plugin"
        data = {"@type": "service", "name": "tedge-can-plugin", "type": "service"}
        self.send_tedge_message(
            MappedMessage(json.dumps(data), topic), retain=True, qos=1
        )

    def register_child_devices(self, devices):
        """Register the child devices with thin-edge.io"""
        for device in devices:
            self.logger.debug("Child device registration for device %s", device["name"])
            topic = f"te/device/{device['name']}//"
            payload = {
                "@type": "child-device",
                "name": device["name"],
                "type": "can-device",
            }
            self.send_tedge_message(
                MappedMessage(json.dumps(payload), topic), retain=True, qos=1
            )


def main():
    """Main"""
    try:
        parser = argparse.ArgumentParser()
        parser.add_argument("-c", "--configdir", required=False)
        parser.add_argument("-l", "--logfile", required=False)
        args = parser.parse_args()
        logging.basicConfig(
            level=logging.INFO,
            format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        )
        if args.configdir is not None:
            config_dir = os.path.abspath(args.configdir)
        else:
            config_dir = None
        poll = CanPoll(config_dir or DEFAULT_FILE_DIR, args.logfile)
        poll.start_polling()
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as main_err:
        logging.error("Unexpected error. %s", main_err, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
