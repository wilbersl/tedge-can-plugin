#!/usr/bin/env python3
"""Can Lister"""
import logging
import threading
import copy
import can

class CanBusBuffer:
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
            print("Waiting for CAN messages...")
            msg = self.bus.recv(timeout=1.0)
            if msg:
                print(msg)
                msg_dict = {
                    "data": msg.data,
                    "timestamp": msg.timestamp,
                }
                with self._lock:
                    self.latest_messages[msg.arbitration_id] = msg_dict

    def get_latest(self, can_id):
        """Gibt die letzte Nachricht einer CAN-ID zurück (oder None)"""
        with self._lock:  # thread-safe read
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

if __name__ == "__main__":
    can_buffer = CanBusBuffer(channel="vcan0")
    can_buffer.start()

    try:
        while True:
            pass
    except KeyboardInterrupt:
        can_buffer.stop()