from typing import Dict, Tuple
from datetime import datetime, date
import asyncio
import httpx
from sqlalchemy import select
from app.config import CBR_URL, BINANCE_URL, USER_AGENT, MONEY, CBR_SOURCE, POLL_INTERVAL_SECONDS, CRYPTO_CODES, CRYPTO_SOURCE, DEFAULT_TIMEOUT
from app.models.item import ItemModel
from app.db.session import get_db
from app.nats.publisher import NatsPublisher
from app.services.parser import parse_cbr_xml
from app.ws.manager import manager


async def fetch_cbr(wanted: Tuple[str, ...]) -> Dict[str, Dict]:
    headers = {
        "User-Agent": USER_AGENT
    }

    try:
        async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
            response = await client.get(CBR_URL, headers=headers)
    except Exception:
        print(
            "Ошибка запроса ЦБ",
            flush=True
        )
        return {
            "RUB": {
                "rate": 1.0,
                "nominal": 1,
                "source": CBR_SOURCE
            }
        }

    if response.status_code != 200:
        print(
            f"CBR вернул {response.status_code}",
            flush=True
        )
        return {
            "RUB": {
                "rate": 1.0,
                "nominal": 1,
                "source": CBR_SOURCE
            }
        }

    try:
        parsed = parse_cbr_xml(response.content, wanted)
    except Exception:
        print(
            f"Ошибка парсинга XML ЦБ",
            flush=True
        )
        parsed = {}

    rates = {
        "RUB": {
            "rate": 1.0,
            "nominal": 1,
            "source": CBR_SOURCE
        }
    }
    rates.update(parsed)
    return rates


async def fetch_binance(symbols: Tuple[str, ...], usd_rub_rate: float) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    headers = {
        "User-Agent": USER_AGENT
    }

    async with httpx.AsyncClient(timeout=DEFAULT_TIMEOUT) as client:
        for sym in symbols:
            pair = f"{sym}USDT"
            params = {"symbol": pair}

            try:
                response = await client.get(
                    BINANCE_URL,
                    headers=headers,
                    params=params
                )
            except (httpx.HTTPError, Exception):
                print(
                    f"Ошибка запроса Binance для {pair}",
                    flush=True
                )
                continue

            if response.status_code is not 200:
                print(
                    f"Binance вернул {response.status_code} для {pair}",
                    flush=True
                )
                continue

            try:
                data = response.json()
            except Exception:
                print(
                    f"Невозможно распарсить ответ Binance для {pair}",
                    flush=True
                )
                continue

            if "price" not in data:
                print(
                    f"В ответе Binance нет поля price для {pair}",
                    flush=True
                )
                continue

            try:
                price_in_usdt = float(data["price"])
            except (ValueError, TypeError):
                print(
                    f"Неверный формат цены от Binance для {pair}",
                    flush=True
                )
                continue

            price_in_rub = price_in_usdt * usd_rub_rate
            result[sym] = {
                "rate": price_in_rub,
                "nominal": 1,
                "source": CRYPTO_SOURCE or "Binance",
            }

    return result


class Poller:
    def __init__(self, app, nats: NatsPublisher):
        self.app = app
        self.nats = nats
        self._task: asyncio.Task | None = None
        self._stopped = asyncio.Event()


    async def process_and_store(self, combined: Dict[str, Dict]):
        async for db in get_db():
            try:
                for code, info in combined.items():
                    try:
                        sql = select(ItemModel).where(ItemModel.code == code)
                        result = await db.execute(sql)
                        item = result.scalar_one_or_none()

                        if item is None:
                            new = ItemModel(
                                code=code,
                                title=code,
                                rate=info["rate"],
                                nominal=info.get("nominal", 1),
                                source=info.get("source", ""),
                                is_crypto=(code in CRYPTO_CODES),
                            )
                            db.add(new)
                            await db.commit()
                            await db.refresh(new)

                            payload = {
                                "type": "created",
                                "item": {
                                    "code": new.code,
                                    "rate": new.rate,
                                    "nominal": new.nominal,
                                    "source": new.source,
                                    "is_crypto": new.is_crypto,
                                },
                            }

                            try:
                                await manager.broadcast(payload)
                            except Exception:
                                print(
                                    "Не удалось отправить сообщение о создании",
                                    flush=True
                                )

                            try:
                                await self.nats.publish("items.updates", payload)
                            except Exception:
                                print(
                                    "Не удалось опубликовать событие в NATS - created",
                                    flush=True
                                )
                        else:
                            if abs(item.rate - info["rate"]) > 1e-9:
                                item.rate = info["rate"]
                                item.nominal = info.get("nominal", item.nominal)
                                item.source = info.get("source", item.source)
                                item.is_crypto = (code in CRYPTO_CODES)
                                item.updated_at = datetime.utcnow()

                                await db.commit()
                                await db.refresh(item)

                                payload = {
                                    "type": "updated",
                                    "item": {
                                        "code": item.code,
                                        "rate": item.rate,
                                        "nominal": item.nominal,
                                        "source": item.source,
                                        "is_crypto": item.is_crypto,
                                    },
                                }

                                try:
                                    await manager.broadcast(payload)
                                except Exception:
                                    print("Не удалось отправить сообщение об обновлении", flush=True)

                                try:
                                    await self.nats.publish("items.updates", payload)
                                except Exception:
                                    print("Не удалось опубликовать событие в NATS (updated)", flush=True)
                    except Exception as exc:
                        print(f"Ошибка обработки кода {code}: {exc}", flush=True)
            finally:
                break


    async def run_once(self):
        try:
            rates = await fetch_cbr(MONEY)

            if "USD" in rates:
                usd_rub = rates["USD"]["rate"]
            else:
                usd_rub = 80.0
                print(
                    "USD не найден в курсах ЦБ, выставляю 80.0 (run)",
                    flush=True
                )

            crypto = await fetch_binance(CRYPTO_CODES, usd_rub)

            combined: Dict[str, Dict] = {}
            combined.update(rates)
            combined.update(crypto)

            await self.process_and_store(combined)
        except Exception:
            print(
                f"Ошибка в run_once",
                flush=True
            )


    async def _loop(self):
        while True:
            try:
                await self.run_once()
            except asyncio.CancelledError:
                break
            except Exception:
                print(
                    f"Ошибка в poll loop",
                    flush=True
                )
            await asyncio.sleep(POLL_INTERVAL_SECONDS)


    def start(self):
        if self._task is None or self._task.done():
            self._task = asyncio.create_task(self._loop())


    async def stop(self):
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                print(
                    "Poller остановлен"
                )
            self._task = None
