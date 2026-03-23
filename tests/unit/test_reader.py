import os
import sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
sys.path.insert(0, parent_dir)
import unittest
from unittest.mock import patch, MagicMock
from tedge_modbus.reader.reader import ModbusPoll
from tedge_modbus.reader.mapper import ModbusMapper


class TestReaderPollingInterval(unittest.TestCase):

    @patch("tedge_modbus.reader.reader.ModbusPoll.read_base_definition")
    @patch("tedge_modbus.reader.reader.ModbusPoll.read_device_definition")
    def setUp(self, mock_read_device, mock_read_base):
        """Set up a ModbusPoll instance with mocked file reading."""
        # Mock config to prevent errors during initialization
        mock_read_base.return_value = {"thinedge": {}, "modbus": {}}
        mock_read_device.return_value = {}

        self.poll = ModbusPoll(config_dir="/tmp/mock_config")
        # Replace the real scheduler with a mock object for testing
        self.poll.poll_scheduler = MagicMock()

    def test_uses_device_specific_poll_interval(self):
        """
        GIVEN a device has a specific pollinterval
        WHEN the poller schedules the next poll for that device
        THEN it should use the device's interval.
        """
        # GIVEN a global poll interval
        self.poll.base_config = {"modbus": {"pollinterval": 5}}
        # AND a device with its own pollinterval
        device_config = {
            "name": "fast_poller",
            "pollinterval": 1,  # This should be used
        }

        mock_poll_model = MagicMock()
        mock_mapper = MagicMock()

        # WHEN poll_device is called
        # We patch get_data_from_device to avoid real network calls
        with patch.object(
            self.poll,
            "get_data_from_device",
            return_value=(None, None, None, None, None),
        ):
            self.poll.poll_device(device_config, mock_poll_model, mock_mapper)

        # THEN the scheduler should be called with the device's interval
        self.poll.poll_scheduler.enter.assert_called_once()
        call_args, _ = self.poll.poll_scheduler.enter.call_args
        # The first argument to enter() is the delay
        self.assertEqual(call_args[0], 1)

    def test_uses_global_poll_interval_as_fallback(self):
        """
        GIVEN a device does NOT have a specific poll_interval
        WHEN the poller schedules the next poll for that device
        THEN it should use the global pollinterval.
        """
        # GIVEN a global poll interval
        self.poll.base_config = {"modbus": {"pollinterval": 5}}  # This should be used
        # AND a device without its own poll_interval
        device_config = {"name": "normal_poller"}

        mock_poll_model = MagicMock()
        mock_mapper = MagicMock()

        # WHEN poll_device is called
        with patch.object(
            self.poll,
            "get_data_from_device",
            return_value=(None, None, None, None, None),
        ):
            self.poll.poll_device(device_config, mock_poll_model, mock_mapper)

        # THEN the scheduler should be called with the global interval
        self.poll.poll_scheduler.enter.assert_called_once()
        call_args, _ = self.poll.poll_scheduler.enter.call_args
        self.assertEqual(call_args[0], 5)

    def test_defaults_to_no_measurement_combination(self):
        """
        GIVEN no global measurement combination
        AND a device with no defined measurement combination with two or more registers with defined measurements
        WHEN poll_device is called
        # THEN there should be more than one send_tedge_message call containing measurements.
        """
        # GIVEN no global measurement combination
        self.poll.base_config = {"modbus": {"pollinterval": 5}}  # This should be used
        # AND a device with no defined measurement combination with two or more registers with defined measurements
        device_config = {
            "name": "test_device",
            "registers": [
                {
                    "number": 0,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor1":{"temp":%% }}'
                    },
                },
                {
                    "number": 1,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor2":{"temp":%% }}'
                    },
                },
            ],
        }

        mapper = ModbusMapper(device_config)

        mock_poll_model = MagicMock()
        self.poll.send_tedge_message = MagicMock()

        # WHEN poll_device is called
        with patch.object(
            self.poll,
            "get_data_from_device",
            return_value=(None, None, [15, 22], None, None),
        ):
            self.poll.poll_device(device_config, mock_poll_model, mapper)

        # THEN there should be more than one send_tedge_message call containing measurements.
        self.assertGreater(
            len(
                [
                    ele
                    for ele in self.poll.send_tedge_message.call_args_list
                    if ele[0][0].topic == "te/device/test_device///m/"
                ]
            ),
            1,
        )

    def test_global_measurement_combination(self):
        """
        GIVEN global measurement combination
        AND a device with no defined measurement combination with two or more registers with defined measurements
        WHEN poll_device is called
        THEN there should be only one send_tedge_message call containing measurements.
        """
        # GIVEN global measurement combination
        self.poll.base_config = {
            "modbus": {"pollinterval": 5, "combinemeasurements": True}
        }  # This should be used
        # AND a device with no defined measurement combination with two or more registers with defined measurements
        device_config = {
            "name": "test_device",
            "registers": [
                {
                    "number": 0,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor1":{"temp":%% }}'
                    },
                },
                {
                    "number": 1,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor2":{"temp":%% }}'
                    },
                },
            ],
        }

        mapper = ModbusMapper(device_config)

        mock_poll_model = MagicMock()
        self.poll.send_tedge_message = MagicMock()

        # WHEN poll_device is called
        with patch.object(
            self.poll,
            "get_data_from_device",
            return_value=(None, None, [15, 22], None, None),
        ):
            self.poll.poll_device(device_config, mock_poll_model, mapper)

        # THEN there should be only one send_tedge_message call containing measurements.
        self.assertEqual(
            len(
                [
                    ele
                    for ele in self.poll.send_tedge_message.call_args_list
                    if ele[0][0].topic == "te/device/test_device///m/"
                ]
            ),
            1,
        )

    def test_device_specific_measurement_combination(self):
        """
        GIVEN no global measurement combination
        AND a device with defined measurement combination with two or more registers with defined measurements
        WHEN poll_device is called
        THEN there should be only one send_tedge_message call containing measurements.
        """
        # GIVEN no global measurement combination
        self.poll.base_config = {"modbus": {"pollinterval": 5}}  # This should be used
        # AND a device with defined measurement combination with two or more registers with defined measurements
        device_config = {
            "name": "test_device",
            "combinemeasurements": True,
            "registers": [
                {
                    "number": 0,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor1":{"temp":%% }}'
                    },
                },
                {
                    "number": 1,
                    "startbit": 0,
                    "nobits": 16,
                    "signed": False,
                    "on_change": False,
                    "measurementmapping": {
                        "templatestring": '{"sensor2":{"temp":%% }}'
                    },
                },
            ],
        }

        mapper = ModbusMapper(device_config)

        mock_poll_model = MagicMock()
        self.poll.send_tedge_message = MagicMock()

        # WHEN poll_device is called
        with patch.object(
            self.poll,
            "get_data_from_device",
            return_value=(None, None, [15, 22], None, None),
        ):
            self.poll.poll_device(device_config, mock_poll_model, mapper)

        # THEN there should be only one send_tedge_message call containing measurements
        self.assertEqual(
            len(
                [
                    ele
                    for ele in self.poll.send_tedge_message.call_args_list
                    if ele[0][0].topic == "te/device/test_device///m/"
                ]
            ),
            1,
        )
