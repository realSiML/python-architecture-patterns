from sqlalchemy import text
from src import model
from sqlalchemy.orm import Session
from abc import ABC, abstractmethod


class AbstractRepository(ABC):
    @abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlRepository(AbstractRepository):
    def __init__(self, session: Session):
        self.session = session

    def add(self, batch: model.Batch):
        self.session.execute(
            text(
                "insert into batches (reference, sku, eta, _purchased_quantity)"
                " values (:reference, :sku, :eta, :quantity)"
            ),
            dict(
                reference=batch.reference,
                sku=batch.sku,
                eta=batch.eta,
                quantity=batch._purchased_quantity,
            ),
        )

    def get(self, reference) -> model.Batch:
        batch_row = self.session.execute(
            text("""
                SELECT reference, sku, _purchased_quantity, eta, id
                FROM batches
                WHERE reference = :reference
            """),
            dict(reference=reference),
        ).first()

        if not batch_row:
            raise ValueError(f"Batch {reference} not found")

        order_lines_rows = self.session.execute(
            text("""
                SELECT ol.orderid, ol.sku, ol.qty
                FROM order_lines ol
                JOIN allocations a ON ol.id = a.orderline_id
                WHERE a.batch_id = :batch_id
            """),
            dict(batch_id=batch_row.id),
        ).all()

        allocations = {model.OrderLine(*row) for row in order_lines_rows}
        batch = model.Batch(
            ref=batch_row.reference,
            sku=batch_row.sku,
            qty=batch_row._purchased_quantity,
            eta=batch_row.eta,
        )
        batch._allocations = allocations

        return batch

    def list(self):
        return self.session.execute(text("select * from batches")).fetchall()
