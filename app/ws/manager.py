from typing import Set
from fastapi import WebSocket
from fastapi.encoders import jsonable_encoder


class ConnectionManager:
    def __init__(self):
        self.active: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.add(ws)

    def disconnect(self, ws: WebSocket):
        self.active.discard(ws)

    async def broadcast(self, message: dict):
        if not self.active:
            return
        data = jsonable_encoder(message)
        bad = []
        for ws in list(self.active):
            try:
                await ws.send_json(data)
            except Exception:
                bad.append(ws)
        for ws in bad:
            self.disconnect(ws)


manager = ConnectionManager()
