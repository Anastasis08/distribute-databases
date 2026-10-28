"""SQLAlchemy ORM mapping of the source tables (schema is owned by infra/postgres/init)."""

from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, DateTime, Integer, Numeric, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from product_contracts import ProductData


class Base(DeclarativeBase):
    pass


class Product(Base):
    __tablename__ = "products"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    sku: Mapped[str] = mapped_column(String(32))
    name: Mapped[str] = mapped_column(String(200))
    category: Mapped[str] = mapped_column(String(100))
    price: Mapped[Decimal] = mapped_column(Numeric(12, 2))
    quantity: Mapped[int] = mapped_column(Integer)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))

    def to_contract(self) -> ProductData:
        return ProductData(
            id=self.id,
            sku=self.sku,
            name=self.name,
            category=self.category,
            price=self.price,
            quantity=self.quantity,
            created_at=self.created_at,
            updated_at=self.updated_at,
        )


class SyncCheckpoint(Base):
    __tablename__ = "sync_checkpoints"

    name: Mapped[str] = mapped_column(Text, primary_key=True)
    last_updated_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_product_id: Mapped[int] = mapped_column(BigInteger)
    saved_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
