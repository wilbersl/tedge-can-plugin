"""
DBC-based CAN Simulator
- Loads a DBC file
- Sends random but valid signal values
- Uses SocketCAN (vcan0 / can0)
"""

import time
import random
import can
import cantools

DBC_PATH = "simulation.dbc"
CAN_INTERFACE = "vcan0"
CYCLE_TIME = 5.0 # seconds


def random_signal_value(signal):
    """
    Generate a random valid value for a CAN signal.
    Respects min/max if defined in DBC.
    """
    if signal.minimum is not None and signal.maximum is not None:
        return random.uniform(signal.minimum, signal.maximum)

    # fallback
    if signal.is_float:
        return random.random() * 100
    else:
        return random.randint(0, 100)


def main():
    print("Loading DBC...")
    db = cantools.database.load_file(DBC_PATH)

    print(f"Opening CAN interface: {CAN_INTERFACE}")
    bus = can.interface.Bus(
        channel=CAN_INTERFACE, interface="socketcan", bitrate=500000
    )

    print("Starting CAN simulation")
    while True:
        for message in db.messages:
            data = {}

            for signal in message.signals:
                data[signal.name] = random_signal_value(signal)

            encoded = message.encode(data)

            msg = can.Message(
                arbitration_id=message.frame_id,
                data=encoded,
                is_extended_id=message.is_extended_frame,
            )

            try:
                bus.send(msg)
                print(f"Sent {msg}")  # Print the raw CAN message
                print(f"Sent {message.name}: {data}")
            except can.CanError as e:
                print(f"CAN send failed: {e}")

        cycle = message.cycle_time / 1000.0 if message.cycle_time else CYCLE_TIME
        time.sleep(cycle)


if __name__ == "__main__":
    main()
