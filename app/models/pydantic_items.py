from pydantic import BaseModel
from datetime import datetime
from typing import Optional


class ItemBase(BaseModel):
    code: str
    title: str
    rate: float
    nominal: int = 1
    source: str
    is_crypto: bool = False


class ItemCreate(ItemBase):
    pass


class Item(ItemBase):
    id: int
    updated_at: datetime

    class Config:
        orm_mode = True


class ItemUpdate(BaseModel):
    title: Optional[str] = None
    rate: Optional[float] = None
    nominal: Optional[int] = None
    source: Optional[str] = None
    is_crypto: Optional[bool] = None
