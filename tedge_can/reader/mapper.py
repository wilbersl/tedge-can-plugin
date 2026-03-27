#!/usr/bin/env python3
"""Modbus mapper"""
import json
import struct
import sys
import math
from datetime import datetime, timezone
from dataclasses import dataclass

topics = {
    "measurement": "te/device/CHILD_ID///m/",
    "event": "te/device/CHILD_ID///e/TYPE",
    "alarm": "te/device/CHILD_ID///a/TYPE",
}


@dataclass
class MappedMessage:
    """Mapped message"""

    data: str = ""
    topic: str = ""
    time: str = datetime.now(timezone.utc).isoformat()

    def serialize(self):
        """Serialize message adding time if not present"""
        if "/cmd/" in self.topic:
            return self.data
        out = json.loads(self.data)
        if "time" not in out:
            out["time"] = self.time
        return json.dumps(out)

    def extend_data(self, other_message):
        """Combine Json data of two messages with the same topic"""
        if self.topic != other_message.topic:
            raise ValueError("Messages need to have the same topic")

        def merge(d1: dict, d2: dict) -> dict:
            """Recursively merge two dictionaries."""
            for k, v in d2.items():
                if k in d1 and isinstance(d1[k], dict) and isinstance(v, dict):
                    d1[k] = merge(d1[k], v)
                else:
                    d1[k] = v
            return d1

        # Load both JSON strings into dictionaries
        d1 = json.loads(self.data)
        d2 = json.loads(other_message.data)
        # Merge the dictionaries
        merged = merge(d1, d2)

        if "time" not in merged:
            merged["time"] = self.time

        # Convert the merged dictionary back to a JSON string and update self.data
        self.data = json.dumps(merged)


class CanMapper:
    """Can mapper"""

    device = None

    def __init__(self, device):
        self.device = device
        self.data = {}

    def validate(self, register_def):
        """Validate definition"""
        start_bit = register_def["startBit"]
        field_len = register_def["noBits"]
        if field_len > 64:
            raise ValueError(
                f"definition of field length too long ({field_len}) "
                f'for register {register_def["number"]} at {start_bit}'
            )
        if register_def.get("datatype", "integer") == "float" and field_len not in (
            16,
            32,
            64,
        ):
            raise ValueError("float values must have a length of 16, 32 or 64")

    def parse_int(self, buffer, signed, mask):
        """parse value to an integer, reversing byte order after masking"""
        field_len = mask.bit_length()
        # Calculate the number of bytes needed for the field
        byte_length = (field_len + 7) // 8

        # Convert masked value to bytes, reverse, and convert back to integer /
        # compatiblity with DBC Littleendian Byteorder and Masking
        value_bytes = buffer.to_bytes(byte_length, byteorder="big")
        buffer = int.from_bytes(value_bytes, byteorder="little", signed=False)

        # Now check for negative values and apply signed logic for non standard length integer
        is_negative = buffer >> (field_len - 1) & 0x01
        if signed and is_negative:
            value = -(((buffer ^ mask) + 1) & mask)
        else:
            value = buffer & mask
        return value

    def parse_float(self, buffer, field_len):
        """parse value to a float"""
        formats = {16: "e", 32: "f", 64: "d"}
        return struct.unpack(
            formats[field_len], buffer.to_bytes(int(field_len / 8), sys.byteorder)
        )[0]

    def map_register(
        self, read_register, register_def: dict, device_combine_measurements=False
    ):
        """Map register"""
        # pylint: disable=too-many-locals
        messages = []
        separate_measurement = None
        start_bit = register_def["startBit"]
        field_len = register_def["noBits"]
        is_little_endian = register_def.get("littleendian", False)
        register_key = f'{register_def["number"]}:{register_def["startBit"]}'
        self.validate(register_def)
        # concat the registers in case we need to read across multiple registers
        buffer = self.buffer_register(read_register, is_little_endian)
        buffer_len = len(read_register) * 8

        # shift and mask for the cases where the start_bit > 0 and
        # we are not reading the whole register as value
        buffer = buffer >> (buffer_len - (start_bit + field_len))

        i = 1
        mask = 1
        while i < field_len:
            mask = (mask << 1) + 0x1
            i = i + 1

        buffer = buffer & mask
        if register_def.get("datatype", "integer") == "float":
            value = self.parse_float(buffer, field_len)
        else:
            value = self.parse_int(buffer, register_def.get("signed", False), mask)

        if register_def.get("measurementmapping") is not None:
            scaled_value = value * register_def.get("factor", 1) + register_def.get(
                "offset", 0
            )

            on_change = register_def.get("on_change", False)

            last_value = self.data.get(register_key)

            has_changed = False
            last_value = self.data.get(register_key)

            if last_value is not None:
                if isinstance(scaled_value, float):
                    has_changed = not isinstance(last_value, float) or not math.isclose(
                        scaled_value, last_value
                    )
                else:
                    has_changed = last_value != scaled_value

            if not on_change or last_value is None or has_changed:
                data = register_def["measurementmapping"]["templatestring"].replace(
                    "%%", str(scaled_value)
                )
                if register_def["measurementmapping"].get(
                    "combinemeasurements", device_combine_measurements
                ):
                    separate_measurement = MappedMessage(
                        data,
                        topics["measurement"].replace(
                            "CHILD_ID", self.device.get("name")
                        ),
                    )
                else:
                    messages.append(
                        MappedMessage(
                            data,
                            topics["measurement"].replace(
                                "CHILD_ID", self.device.get("name")
                            ),
                        )
                    )

            value = scaled_value
        if register_def.get("alarmmapping") is not None:
            messages.extend(
                self.check_alarm(value, register_def.get("alarmmapping"), register_key)
            )
        if register_def.get("eventmapping") is not None:
            messages.extend(
                self.check_event(value, register_def.get("eventmapping"), register_key)
            )

        self.data[register_key] = value
        return messages, separate_measurement

    def check_alarm(self, value, alarm_mapping, register_key):
        """Check alarm"""
        messages = []
        old_data = self.data.get(register_key)
        # raise alarm if bit is 1
        if (old_data is None or old_data == 0) and value > 0:
            severity = alarm_mapping["severity"].lower()
            alarm_type = alarm_mapping.get("type", "")
            text = alarm_mapping["text"]
            topic = topics["alarm"]
            topic = topic.replace("CHILD_ID", self.device.get("name"))
            topic = topic.replace("TYPE", alarm_type)
            data = {
                "text": text,
                "severity": severity,
                "time": datetime.now(timezone.utc).isoformat(),
            }
            messages.append(MappedMessage(json.dumps(data), topic))
        return messages

    def check_event(self, value, event_mapping, register_key):
        """Check event"""
        messages = []
        old_data = self.data.get(register_key)
        # raise event if value changed
        if old_data is None or old_data != value:
            eventtype = event_mapping.get("type", "")
            text = event_mapping["text"]
            topic = topics["event"]
            topic = topic.replace("CHILD_ID", self.device.get("name"))
            topic = topic.replace("TYPE", eventtype)
            data = {"text": text, "time": datetime.now(timezone.utc).isoformat()}
            messages.append(MappedMessage(json.dumps(data), topic))
        return messages

    @staticmethod
    def buffer_register(register: list, is_little_word_endian):
        """Buffer register"""
        buf = 0x00

        if is_little_word_endian:
            for reg in reversed(register):
                buf = (buf << 8) | reg
        else:
            for reg in register:
                buf = (buf << 8) | reg

        return buf
