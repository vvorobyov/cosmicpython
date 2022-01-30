import abc

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert
from batches.domain import model

from .db_tables import batches, order_lines, allocations


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def add(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, session):
        self._connection = session

    @property
    def _conn(self) -> sa.engine.Connection:
        return self._connection.conn

    @property
    def _trn(self) -> sa.engine.Transaction:
        return self._connection.transaction

    def add(self, batch: model.Batch):
        batch_id = self._get_or_create_batch(batch)
        if batch._allocations:
            lines_id = self._create_order_lines(batch._allocations)
            self._update_allocations(batch_id, lines_id)

    def _update_allocations(self, batch_id: int, lines_id: list[int]):
        values = [
            dict(batch_id=batch_id,
                 orderline_id=line_id)
            for line_id in lines_id
        ]
        with self._conn.begin():
            self._conn.execute(
                insert(allocations).
                values(values).
                on_conflict_do_nothing()
            )
            self._conn.execute(
                sa.delete(allocations).
                where(allocations.c.batch_id == batch_id,
                      allocations.c.orderline_id.not_in(lines_id))
            )

    def _create_order_lines(self, lines: tuple[model.OrderLine,...]) -> list[int]:
        values = [
            dict(
                sku=line.sku,
                orderid=line.orderid,
                qty=line.qty
            )
            for line in lines
        ]
        or_conditions = [
            sa.and_(order_lines.c.orderid == line.orderid, order_lines.c.sku == line.sku)
            for line in lines
        ]
        with self._conn.begin():
            self._conn.execute(
                insert(order_lines).
                values(values).
                on_conflict_do_nothing().
                returning(order_lines.c.id)
            )
            cursor = self._conn.execute(
                sa.select([order_lines.c.id]).
                where(sa.or_(*or_conditions))
            )
            return [row.id for row in cursor.all()]

    def _get_or_create_batch(self, batch: model.Batch):
        with self._conn.begin():
            cursor = self._conn.execute(
                insert(batches).values(
                    reference=batch.reference,
                    sku=batch.sku,
                    eta=batch.eta,
                    purchased_quantity=batch._purchased_quantity,
                ).on_conflict_do_nothing().returning(batches.c.id)
            )
            row = cursor.one_or_none()
            if row:
                return row.id
            cursor = self._conn.execute(
                sa.select([batches.c.id]).where(batches.c.reference == batch.reference)
            )
            return cursor.one().id

    def get(self, reference) -> model.Batch:
        row = self._conn.execute(
            batches.select(batches.c.reference == reference)
        ).one()
        batch = model.Batch(
            row.reference,
            row.sku,
            row.purchased_quantity,
            row.eta
        )
        [batch.allocate(line) for line in self._get_order_lines(row.id)]
        return batch

    def _get_order_lines(self, batch_id: int):
        stmt = sa.select([
            order_lines.c.sku,
            order_lines.c.qty,
            order_lines.c.orderid,
        ]).select_from(
            order_lines.join(allocations)
        ).where(allocations.c.batch_id == batch_id)
        rows = self._conn.execute(stmt).all()
        return [model.OrderLine(row.orderid, row.sku, row.qty)
                for row in rows]
