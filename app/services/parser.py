import xml.etree.ElementTree as ElementTree
import requests
from datetime import datetime, date
import asyncio
from typing import Dict, Tuple
from sqlalchemy import select
from app.config import CBR_URL, USER_AGENT, MONEY, CBR_SOURCE, POLL_INTERVAL_SECONDS, CRYPTO_CODES, CRYPTO_SOURCE, BINANCE_URL, DEFAULT_TIMEOUT
from app.models.item import ItemModel
from app.ws.manager import manager


cache: Dict[str, Dict] = {}


def format_date(dt: date) -> str:
    return dt.strftime("%d/%m/%Y")


def parse_cbr_xml(content: bytes, wanted: Tuple[str, ...]) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}
    try:
        root = ElementTree.fromstring(content)
    except Exception:
        return result

    for val in root.findall(".//Valute"):
        char_node = val.find("CharCode")
        if char_node is None or char_node.text is None:
            continue
        code = char_node.text.strip()
        if code not in wanted:
            continue

        value_node = val.find("Value")
        if value_node is None or value_node.text is None:
            value_node = val.find("VunitRate")

        nominal_node = val.find("Nominal")
        try:
            raw_value = value_node.text.strip().replace(",", ".")
            value_float = float(raw_value)
        except Exception:
            continue

        try:
            nominal = int(nominal_node.text.strip()) if nominal_node is not None and nominal_node.text else 1
        except Exception:
            nominal = 1

        rate = value_float / nominal
        result[code] = {
            "rate": rate,
            "nominal": nominal,
            "source": "ЦБ РФ"
        }

    return result


def get_exchange_rates_for_date_sync(dt: date) -> Dict[str, Dict]:
    formatted = format_date(dt)
    if formatted in cache:
        return cache[formatted]

    url = CBR_URL.format(
        date=formatted
    )
    headers = {
        "User-Agent": USER_AGENT
    }
    try:
        response = requests.get(
            url,
            headers=headers,
            timeout=DEFAULT_TIMEOUT
        )
        print(response)
    except requests.RequestException as exc:
        print(
            f"Ошибка запроса ЦБ: {exc}",
            flush=True
        )
        cache[formatted] = {
            "RUB": {
                "rate": 80.0,
                "nominal": 80,
                "source": CBR_SOURCE
            }
        }
        return cache[formatted]

    if response.status_code is not 200:
        cache[formatted] = {
            "RUB": {
                "rate": 80.0,
                "nominal": 80,
                "source": CBR_SOURCE
            }
        }
        return cache[formatted]

    rates: Dict[str, Dict] = {
        "RUB": {
            "rate": 80.0,
            "nominal": 80,
            "source": CBR_SOURCE
        }
    }

    parsed = parse_cbr_xml(response.content, MONEY)
    rates.update(parsed)
    cache[formatted] = rates

    return rates


def get_crypto_rates_from_binance(symbols: Tuple[str, ...], usd_rub_rate: float) -> Dict[str, Dict]:
    result: Dict[str, Dict] = {}

    headers = {
        "User-Agent": USER_AGENT
    }

    for sym in symbols:
        pair = f"{sym}USDT"
        params = {
            "symbol": pair
        }

        try:
            response = requests.get(
                BINANCE_URL,
                headers=headers,
                params=params,
                timeout=DEFAULT_TIMEOUT
            )
        except requests.RequestException:
            print(
                "Ошибка запроса Binance для {pair}",
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
        except ValueError:
            print(
                "Невозможно распарсить ответ Binance для {pair}",
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
            "nominal": 80,
            "source": CRYPTO_SOURCE or "binance"
        }
    return result


async def poll_loop(app):
    while True:
        try:
            rates = await asyncio.to_thread(get_exchange_rates_for_date_sync, datetime.utcnow().date())

            if "USD" in rates:
                usd_rub = rates["USD"]["rate"]
            else:
                usd_rub = 8.0
                print(
                    "USD не найден в курсах ЦБ, выставляю 80.0 (poll)",
                    flush=True
                )

            crypto = await asyncio.to_thread(get_crypto_rates_from_binance, CRYPTO_CODES, usd_rub)
            combined: Dict[str, Dict] = {}
            combined.update(rates)
            combined.update(crypto)

            async with app.state.db() as db:
                for code, info in combined.items():
                    item_model = select(ItemModel).where(ItemModel.code == code)
                    result = await db.execute(item_model)
                    item = result.scalar_one_or_none()

                    if item is None:
                        new = ItemModel(
                            code=code,
                            title=code,
                            rate=info["rate"],
                            nominal=info.get("nominal", 1),
                            source=info.get("source", ""),
                            is_crypto=(code in CRYPTO_CODES)
                        )
                        db.add(new)
                        await db.commit()
                        await db.refresh(new)

                        try:
                            await manager.broadcast({
                                    "type": "created",
                                    "item": {
                                        "code": new.code,
                                        "rate": new.rate,
                                        "nominal": new.nominal,
                                        "source": new.source,
                                        "is_crypto": new.is_crypto
                                    }
                            })
                        except Exception:
                            print(
                                "Не удалось отправить сообщение о создании",
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

                            try:
                                await manager.broadcast({
                                    "type": "updated",
                                    "item": {
                                        "code": item.code,
                                        "rate": item.rate,
                                        "nominal": item.nominal,
                                        "source": item.source,
                                        "is_crypto": item.is_crypto
                                    }
                                })
                            except Exception:
                                print(
                                    "Не удалось отправить сообщение об обновлении",
                                    flush=True
                                )

            await asyncio.sleep(POLL_INTERVAL_SECONDS)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(
                f"Ошибка в poll_loop: {exc}",
                flush=True
            )
            await asyncio.sleep(POLL_INTERVAL_SECONDS)
