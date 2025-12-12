from datetime import datetime
from typing import Optional
from sqlalchemy import Integer, Float, DateTime, Column, String, Boolean
from sqlmodel import SQLModel, Field

class ItemModel(SQLModel, table=True):
    __tablename__ = "items"

    id: Optional[int] = Field(default=None, primary_key=True)
    code: str = Field(sa_column=Column(String, nullable=False, unique=True))
    title: str = Field(sa_column=Column(String, nullable=False))
    rate: float = Field(default=0.0, sa_column=Column(Float, nullable=False))
    nominal: int = Field(default=1, sa_column=Column(Integer, nullable=False))
    source: str = Field(default="", sa_column=Column(String, nullable=False))
    is_crypto: bool = Field(default=False, sa_column=Column(Boolean, nullable=False))
    updated_at: datetime = Field(default_factory=datetime.utcnow, sa_column=Column(DateTime, nullable=False))
