import json
from nats.aio.client import Client as Nats
from app.db.session import get_db
from sqlalchemy import select
from app.models.item import ItemModel
from app.ws.manager import manager


async def on_message(msg):
    try:
        data = json.loads(msg.data.decode("utf-8"))
    except Exception:
        print(
            "Ошибка парсинга сообщения NATS",
            flush=True
        )
        return

    print(
        f"NATS получил: {data}",
        flush=True
    )

    item_data = data.get("item")
    if not item_data:
        return

    code = item_data.get("code")
    if not code:
        return

    async for db in get_db():
        try:
            sql = select(ItemModel).where(ItemModel.code == code)
            result = await db.execute(sql)
            item = result.scalar_one_or_none()

            if item is None:
                new = ItemModel(
                    code=item_data["code"],
                    title=item_data["code"],
                    rate=item_data["rate"],
                    nominal=item_data["nominal"],
                    source=item_data["source"],
                    is_crypto=item_data["is_crypto"],
                )
                db.add(new)
                await db.commit()
                await db.refresh(new)
                print(
                    f"Создан элемент из NATS: {item_data.get('code')}",
                    flush=True
                )
            else:
                item.rate = item_data["rate"]
                item.nominal = item_data["nominal"]
                item.source = item_data["source"]
                item.is_crypto = item_data["is_crypto"]
                await db.commit()
                await db.refresh(item)
                print(
                    f"Обновлён элемент из NATS: {code}",
                    flush=True
                )
        finally:
            break

    try:
        await manager.broadcast(data)
    except Exception:
        print(
            "Не удалось отправить сообщение в NATS",
            flush=True
        )


class NatsSubscriber:
    def __init__(self, servers: list[str]):
        self._servers = servers
        self._nats_client: Nats | None = None

    async def connect(self):
        nats_client = Nats()
        await nats_client.connect(servers=self._servers)
        self._nats_client = nats_client
        await nats_client.subscribe("items.updates", cb=on_message)
        print(f"NATS подписался", flush=True)
