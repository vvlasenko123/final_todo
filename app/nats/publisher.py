import asyncio
import json
from nats.aio.client import Client as Nats


class NatsPublisher:
    def __init__(self, servers: list[str]):
        self._servers = servers
        self._nats_client: Nats | None = None
        self._lock = asyncio.Lock()


    async def connect(self):
        async with self._lock:
            if self._nats_client is not None and self._nats_client.is_connected:
                return
            nats_client = Nats()
            await nats_client.connect(servers=self._servers)
            self._nats_client = nats_client


    async def close(self):
        async with self._lock:
            if self._nats_client is None:
                return
            try:
                await self._nats_client.drain()
            except Exception:
                pass
            try:
                await self._nats_client.close()
            except Exception:
                pass
            self._nats_client = None


    async def publish(self, subject: str, message: dict):
        if self._nats_client is None or not self._nats_client.is_connected:
            await self.connect()

        payload = (json.dumps(message).encode("utf-8"))
        await self._nats_client.publish(subject, payload)
