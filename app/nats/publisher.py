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
            print(f"NATS publisher подключен, servers={self._servers}", flush=True)


    async def close(self):
        async with self._lock:
            if self._nats_client is None:
                return
            try:
                await self._nats_client.drain()
            except Exception:
                print("Ошибка NATS клиента", flush=True)
            try:
                await self._nats_client.close()
            except Exception:
                print("Ошибка при закрытии NATS клиента", flush=True)
            self._nats_client = None
            print("NATS publisher отключен", flush=True)


    async def publish(self, subject: str, message: dict):
        if self._nats_client is None or not self._nats_client.is_connected:
            await self.connect()

        payload = (json.dumps(message).encode("utf-8"))
        try:
            await self._nats_client.publish(subject, payload)
            print(f"Опубликовано в NATS: {subject}, message={message}", flush=True)
        except Exception:
            print(f"Ошибка при публикации в NATS: {subject}", flush=True)
            raise
