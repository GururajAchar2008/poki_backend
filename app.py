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
            send_json(connection, {"type": "error", "message": "This private room already has two connected devices."})
            return
        rooms[room].add(connection)

    participant = {"name": "Poki friend", "deviceId": "unknown"}
    try:
        raw_hello = connection.receive()
        if raw_hello:
            hello = json.loads(raw_hello)
            if hello.get("type") == "hello":
                participant["name"] = str(hello.get("name") or participant["name"])[:60]
                participant["deviceId"] = str(hello.get("deviceId") or participant["deviceId"])[:100]
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
            if not rooms[room]:
                rooms.pop(room, None)
        broadcast(room, {"type": "presence", "online": False, "name": participant["name"]})


@app.get("/health")
def health():
    return {"ok": True, "service": "poki-signaling", "persistent_chat_storage": False}


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
