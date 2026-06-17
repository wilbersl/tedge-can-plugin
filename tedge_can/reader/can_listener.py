#!/usr/bin/env python3
"""Can Lister"""
import threading
import copy
import subprocess
import can
import time

class CanBusBuffer:
    """
    CAN-Bus Reader, der kontinuierlich Nachrichten liest,
    ein Dictionary mit den neuesten Daten je CAN-ID pflegt
    und auch Nachrichten senden kann.

    z. B. 'ip -d link show can0' zeigt die aktuellen Einstellungen von can0 an und ob termination einstellbar ist.

    Args:
    channel (str): CAN-Interface, z. B. 'can0'
    bustype (str): CAN-Interface-Typ, z. B. 'socketcan'
    bitrate (int): Optional, Bitrate des CAN-Busses, z. B. 250000
    listen_only (bool): Optional, ob im Listen-Only-Modus betrieben werden soll
    termination (int): Optional, Integrierter Abschlusswiderstand in Ohm (0, 120 oder None für unverändert/nicht unterstützt)
    """

    def __init__(
        self, channel="can0", bustype="socketcan", bitrate: int | None=None, listen_only: bool | None=None, termination: int | None=None
    ):
        if bustype == "socketcan":
            try:
                subprocess.run(
                    ["sudo", "ip", "link", "set", channel, "down"], check=True
                )
                print(f"{channel} ist jetzt DOWN.")
            except subprocess.CalledProcessError as e:
                print(f"Fehler beim Abschalten von {channel}: {e}")
                raise e
            try:
                command = [
                            "sudo",
                            "ip",
                            "link",
                            "set",
                            channel,
                            "up",
                            "type",
                            "can"
                ]

                if bitrate is not None:
                    command += ["bitrate", str(bitrate)]
                if listen_only is not None:
                    command += ["listen-only", "on" if listen_only else "off"]
                if termination is not None:
                    command += ["termination", str(termination)]

                subprocess.run(
                    command,
                    check=True,
                )
                print(f"{channel} ist jetzt UP.")
            except subprocess.CalledProcessError as e:
                print(f"Fehler beim Hochsetzen von {channel}: {e}")
                raise e

        self.bus = can.interface.Bus(channel=channel, bustype=bustype)
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
    can_buffer = CanBusBuffer(channel="can0", bitrate=250000, listen_only=False, termination=None)
    can_buffer.start()

    try:
        while True:
            print(can_buffer.get_all_latest())
            time.sleep(1)
    except KeyboardInterrupt:
        can_buffer.stop()
