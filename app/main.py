from typing import Any, cast
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlmodel import SQLModel
from app.api import items
from app.config import NATS_SERVERS
from app.db.session import engine
from app.nats.publisher import NatsPublisher
from app.nats.subscriber import NatsSubscriber
from app.tasks.poller import Poller


app = FastAPI(title="Money Parser API", version="1.0")


app.include_router(items.router)


app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


app_state = cast(Any, getattr(app, "state"))


@app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    publisher = NatsPublisher(servers=NATS_SERVERS)
    await publisher.connect()
    app_state.nats_publisher = publisher

    subscriber = NatsSubscriber(servers=NATS_SERVERS)
    await subscriber.connect()
    app_state.nats_subscriber = subscriber

    poller = Poller(app=app, nats=publisher)
    poller.start()
    app_state.poller = poller


@app.on_event("shutdown")
async def on_shutdown():
    if getattr(app_state, "poller", None) is not None:
        await app_state.poller.stop()

    if getattr(app_state, "nats_publisher", None) is not None:
        await app_state.nats_publisher.close()