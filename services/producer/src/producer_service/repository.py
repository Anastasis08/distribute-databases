"""Data access for change detection (products) and progress tracking (sync_checkpoints)."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from sqlalchemy import Engine, create_engine, func, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import sessionmaker

from product_contracts import ProductData

from .models import Product, SyncCheckpoint


@dataclass(frozen=True, slots=True)
class Watermark:
    """Position in the ordered stream of changes: (updated_at, id) of the last published row."""

    updated_at: datetime
    product_id: int

    def __str__(self) -> str:
        return f"(updated_at={self.updated_at.isoformat()}, id={self.product_id})"

    @classmethod
    def initial(cls) -> Watermark:
        return cls(datetime(1970, 1, 1, tzinfo=UTC), 0)

    @classmethod
    def of(cls, product: ProductData) -> Watermark:
        return cls(product.updated_at, product.id)


def create_db_engine(database_url: str) -> Engine:
    return create_engine(database_url, pool_pre_ping=True, pool_size=2, max_overflow=0)


class ProductRepository:
    def __init__(self, engine: Engine) -> None:
        self._sessions = sessionmaker(engine, expire_on_commit=False)

    def ping(self) -> None:
        with self._sessions() as session:
            session.execute(text("SELECT 1"))

    def fetch_changes(self, after: Watermark, limit: int, settle_delay: timedelta) -> list[ProductData]:
        """Rows inserted/updated after the watermark, oldest first (keyset pagination).

        Rows modified during the last `settle_delay` are left for the next poll, so a
        transaction that commits slightly later with an older updated_at is not missed.
        """
        cutoff = func.clock_timestamp() - settle_delay
        query = (
            select(Product)
            .where(tuple_(Product.updated_at, Product.id) > tuple_(after.updated_at, after.product_id))
            .where(Product.updated_at <= cutoff)
            .order_by(Product.updated_at, Product.id)
            .limit(limit)
        )
        with self._sessions() as session:
            return [row.to_contract() for row in session.scalars(query)]

    def get_current(self, product_ids: list[int]) -> list[ProductData]:
        query = select(Product).where(Product.id.in_(product_ids)).order_by(Product.id)
        with self._sessions() as session:
            return [row.to_contract() for row in session.scalars(query)]


class CheckpointRepository:
    def __init__(self, engine: Engine, name: str) -> None:
        self._sessions = sessionmaker(engine)
        self._name = name

    def load(self) -> Watermark:
        with self._sessions() as session:
            checkpoint = session.get(SyncCheckpoint, self._name)
        if checkpoint is None:
            return Watermark.initial()
        return Watermark(checkpoint.last_updated_at, checkpoint.last_product_id)

    def save(self, watermark: Watermark) -> None:
        statement = insert(SyncCheckpoint).values(
            name=self._name,
            last_updated_at=watermark.updated_at,
            last_product_id=watermark.product_id,
            saved_at=func.now(),
        )
        statement = statement.on_conflict_do_update(
            index_elements=[SyncCheckpoint.name],
            set_={
                "last_updated_at": statement.excluded.last_updated_at,
                "last_product_id": statement.excluded.last_product_id,
                "saved_at": statement.excluded.saved_at,
            },
        )
        with self._sessions.begin() as session:
            session.execute(statement)

    def reset(self) -> None:
        with self._sessions.begin() as session:
            obj = session.get(SyncCheckpoint, self._name)
            if obj is not None:
                session.delete(obj)
