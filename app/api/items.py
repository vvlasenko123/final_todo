from typing import List
from fastapi import Depends, HTTPException, WebSocket, WebSocketDisconnect, APIRouter, Request
from fastapi.encoders import jsonable_encoder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from starlette import status
from app.config import DEFAULT_TIMEOUT
from app.db.session import get_db
from app.models.item import ItemModel
from app.models.pydantic_items import Item, ItemCreate, ItemUpdate
from app.tasks.poller import Poller
from app.ws.manager import manager


router = APIRouter()


@router.get("/items", response_model=List[Item])
async def get_items(db: AsyncSession = Depends(get_db)):
    sql = select(ItemModel)
    items = await db.execute(sql)
    return items.scalars().all()


@router.get("/items/{item_id}", response_model=Item)
async def get_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(ItemModel, item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Элемент не найден"
        )
    return item


@router.post("/items", response_model=Item, status_code=status.HTTP_201_CREATED)
async def create_item(item: ItemCreate, request: Request, db: AsyncSession = Depends(get_db)):
    if not isinstance(item.code, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Код должен быть строкой"
        )
    code_str = item.code.strip()
    if code_str == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Код не должен быть пустым"
        )
    if not code_str.isalpha():
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Код может содержать только буквы"
        )

    if not isinstance(item.title, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Название должно быть строкой, желательно описывающей валюту"
        )
    title_str = item.title.strip()
    if title_str == "":
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Название не может быть пустым"
        )

    if not (isinstance(item.rate, (int, float))):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Цена должна быть только числом"
        )
    if item.rate < 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Цена не может быть отрицательной"
        )

    if not isinstance(item.nominal, int):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Номинал должен быть целым числом"
        )
    if item.nominal < 1:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Номинал должен быть не менее единицы"
        )

    if not isinstance(item.source, str):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Источник данных должен быть строкой"
        )

    if not isinstance(item.is_crypto, bool):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Указание крипты должно быть булевым значением"
        )

    new_item = ItemModel(
        code=item.code,
        title=item.title,
        rate=item.rate,
        nominal=item.nominal,
        source=item.source,
        is_crypto=item.is_crypto
    )

    db.add(new_item)
    try:
        await db.commit()
    except Exception as exc:
        from sqlalchemy.exc import IntegrityError
        try:
            await db.rollback()
        except Exception:
            pass

        if isinstance(exc, IntegrityError):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Элемент с таким code уже существует"
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при сохранении"
        )

    await db.refresh(new_item)

    payload = {
        "type": "created",
        "item": jsonable_encoder(new_item)
    }
    try:
        client_present = await manager.wait_for_clients(timeout=DEFAULT_TIMEOUT)
        if not client_present:
            print("Нет WebSocket клиентов после ожидания 10 секунд", flush=True)
        else:
            await manager.broadcast(payload)
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при отправке сообщения по ws"
        )

    nats = getattr(request.app.state, "nats_publisher", None)
    if nats is not None:
        try:
            await nats.publish("items.updates", payload)
        except Exception:
            print(
                "Не удалось опубликовать событие в NATS - created",
                flush=True
            )

    return new_item


@router.patch("/items/{item_id}", response_model=Item)
async def patch_item(item_id: int, patch: ItemUpdate, db: AsyncSession = Depends(get_db)):
    item = await db.get(ItemModel, item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Элемент не найден"
        )

    changed = False

    if patch.title is not None:
        item.title = patch.title
        changed = True
    if patch.rate is not None:
        item.rate = patch.rate
        changed = True
    if patch.nominal is not None:
        item.nominal = patch.nominal
        changed = True
    if patch.source is not None:
        item.source = patch.source
        changed = True
    if patch.is_crypto is not None:
        item.is_crypto = patch.is_crypto
        changed = True

    if changed:
        await db.commit()
        await db.refresh(item)
        try:
            client_present = await manager.wait_for_clients(timeout=DEFAULT_TIMEOUT)
            if not client_present:
                print("Нет WebSocket клиентов после ожидания 10 секунд", flush=True)
            else:
                await manager.broadcast({
                    "type": "updated",
                    "item": jsonable_encoder(item)
                })
        except Exception:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Ошибка при отправке сообщения"
            )

    return item


@router.delete("/items/{item_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_item(item_id: int, db: AsyncSession = Depends(get_db)):
    item = await db.get(ItemModel, item_id)
    if not item:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Элемент не найден"
        )

    await db.delete(item)
    await db.commit()

    try:
        client_present = await manager.wait_for_clients(timeout=DEFAULT_TIMEOUT)
        if not client_present:
            print("Нет WebSocket клиентов после ожидания 10 секунд", flush=True)
        else:
            await manager.broadcast({
                "type": "deleted",
                "id": item_id
            })
    except Exception:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Ошибка при отправке сообщения"
        )


@router.websocket("/ws/items")
async def ws_items(ws: WebSocket):
    print(
        "WebSocket вызван, manager_id=", id(manager), "client=", getattr(ws, "client", None),
        flush=True
    )
    await manager.connect(ws)
    try:
        while True:
            try:
                text = await ws.receive_text()
                print("WebSocker ресейвит текст:", getattr(ws, "client", None), "text=", text, flush=True)
            except WebSocketDisconnect:
                print("WebSocketDisconnect", flush=True)
                break
            except Exception as e:
                print("WebSocket error:", e, flush=True)
                break
    finally:
        manager.disconnect(ws)
        print("WS handler finished for client=", getattr(ws, "client", None), flush=True)



@router.post("/tasks/run", status_code=status.HTTP_202_ACCEPTED)
async def run_task(request: Request):
    poller: Poller | None = getattr(request.app.state, "poller", None)

    if poller is None:
        raise HTTPException(
            status_code=500,
            detail="Фоновая задача не запущена"
        )

    try:
        await poller.run_once()
    except Exception:
        raise HTTPException(
            status_code=500,
            detail="Ошибка при выполнении задач"
        )

    return "Задача выполнилась успешно"
