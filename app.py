"""Stateless signaling and presence relay for Poki.

This server never writes messages or media to disk. It only keeps active
WebSocket connections in memory so two paired browsers can find each other.
"""
from collections import defaultdict
import json
from threading import Lock

from flask import Flask
from flask_cors import CORS
from flask_sock import Sock

app = Flask(__name__)
CORS(app)
sock = Sock(app)
rooms: dict[str, set] = defaultdict(set)
participants: dict[object, dict] = {}
room_pin_hashes: dict[str, str] = {}
room_lock = Lock()


def send_json(connection, payload):
    try:
        connection.send(json.dumps(payload))
        return True
    except Exception:
        return False


def broadcast(room: str, payload: dict, exclude=None):
    with room_lock:
        peers = list(rooms.get(room, set()))
    for peer in peers:
        if peer is not exclude:
            send_json(peer, payload)


@sock.route("/ws/<room>")
def websocket(connection, room):
    with room_lock:
        if len(rooms[room]) >= 2:
            send_json(connection, {"type": "error", "error": "This private room already has two connected devices."})
            return
        rooms[room].add(connection)

    participant = {"name": "Poki friend", "deviceId": "unknown", "pinHash": ""}
    registered = False
    try:
        raw_hello = connection.receive()
        if raw_hello:
            try:
                hello = json.loads(raw_hello)
            except json.JSONDecodeError:
                send_json(connection, {"type": "error", "error": "Invalid connection handshake."})
                with room_lock:
                    rooms[room].discard(connection)
                return
            if hello.get("type") == "hello":
                participant["name"] = str(hello.get("name") or participant["name"])[:60]
                participant["deviceId"] = str(hello.get("deviceId") or participant["deviceId"])[:100]
                participant["pinHash"] = str(hello.get("pinHash") or "")[:128]

        if participant["deviceId"] == "unknown" or not participant["pinHash"]:
            send_json(connection, {"type": "error", "error": "A device ID and calculator PIN are required to join this room."})
            with room_lock:
                rooms[room].discard(connection)
            return

        with room_lock:
            expected_pin = room_pin_hashes.get(room)
            if expected_pin and participant["pinHash"] != expected_pin:
                send_json(connection, {"type": "error", "error": "This room uses a different unlock PIN."})
                rooms[room].discard(connection)
                return
            room_pin_hashes.setdefault(room, participant["pinHash"])
            existing_participants = [participants[peer] for peer in rooms[room] if peer is not connection and peer in participants]
            participants[connection] = participant
            registered = True

        for existing in existing_participants:
            send_json(connection, {"type": "presence", "online": True, "name": existing["name"]})
        send_json(connection, {"type": "connected", "online": True})
        broadcast(room, {"type": "presence", "online": True, "name": participant["name"]}, exclude=connection)

        while True:
            raw_packet = connection.receive()
            if raw_packet is None:
                break
            try:
                packet = json.loads(raw_packet)
            except json.JSONDecodeError:
                continue
            packet_type = packet.get("type")
            if packet_type in {"message", "signal"}:
                # Relay only. No persistence, indexing, or media storage happens here.
                broadcast(room, {**packet, "senderId": participant["deviceId"]}, exclude=connection)
            elif packet_type == "ping":
                send_json(connection, {"type": "pong"})
    finally:
        with room_lock:
            rooms[room].discard(connection)
            participants.pop(connection, None)
            if not rooms[room]:
                rooms.pop(room, None)
                room_pin_hashes.pop(room, None)
        if registered:
            broadcast(room, {"type": "presence", "online": False, "name": participant["name"]})


@app.get("/health")
def health():
    return {"ok": True, "service": "poki-signaling", "persistent_chat_storage": False}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
