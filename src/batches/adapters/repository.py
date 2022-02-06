import abc

import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert


from batches.domain import model

from .db_tables import batches, order_lines, allocations


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def save(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get(self, reference) -> model.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def list(self) -> list[model.Batch]:
        raise NotImplementedError


def _get_select_batches_statement(condition=None):
    order_lines_col = sa.func.array_agg(
        sa.func.jsonb_build_object(
            'id', order_lines.c.id,
            'sku', order_lines.c.sku,
            'qty', order_lines.c.qty,
            'orderid', order_lines.c.orderid,
        )).label('order_lines')

    join_stmt = batches.\
        join(allocations, batches.c.id == allocations.c.batch_id, isouter=True).\
        join(order_lines, allocations.c.orderline_id == order_lines.c.id, isouter=True)
    stmt = sa.select([
        batches.c.id,
        batches.c.reference,
        batches.c.sku,
        batches.c.purchased_quantity,
        batches.c.eta,
        order_lines_col
    ]).select_from(join_stmt).group_by(batches)
    if condition is not None:
        stmt = stmt.where(condition)
    return stmt


def _row_to_batch(row) -> model.Batch:
    batch = model.Batch(
        row.reference,
        row.sku,
        row.purchased_quantity,
        row.eta
    )
    object.__setattr__(batch, '_storage__id', row.id)
    [batch.allocate(model.OrderLine(line['orderid'], line['sku'], line['qty']))
     for line in row.order_lines if line['id'] is not None]
    return batch


class SqlAlchemyRepository(AbstractRepository):
    def __init__(self, connection: sa.engine.Connection):
        self._connection = connection

    def _create_batch(self, batch: model.Batch):
        self._connection.begin()
        cursor = self._connection.execute(
            insert(batches).values(
                reference=batch.reference,
                sku=batch.sku,
                eta=batch.eta,
                purchased_quantity=batch._purchased_quantity,
            ).returning(batches.c.id)
        )
        row = cursor.one()
        object.__setattr__(batch, "_storage__id", row.id)
        return row.id

    def save(self, batch: model.Batch):
        batch_id = getattr(batch, '_storage__id', None)
        if batch_id is None:
            batch_id = self._create_batch(batch)
        if batch._allocations:
            lines_id = self._create_order_lines(batch._allocations)
            self._update_allocations(batch_id, lines_id)

    def _update_allocations(self, batch_id: int, lines_id: list[int]):
        values = [
            dict(batch_id=batch_id,
                 orderline_id=line_id)
            for line_id in lines_id
        ]
        self._connection.begin()
        self._connection.execute(
            insert(allocations).
            values(values).
            on_conflict_do_nothing()
        )
        self._connection.execute(
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
        self._connection.begin()
        self._connection.execute(
            insert(order_lines).
            values(values).
            on_conflict_do_nothing()
        )
        cursor = self._connection.execute(
            sa.select([order_lines.c.id]).
            where(sa.or_(*or_conditions))
        )
        return [row.id for row in cursor.all()]

    def get(self, reference) -> model.Batch:
        stmt = _get_select_batches_statement(batches.c.reference == reference)
        row = self._connection.execute(stmt).one()
        return _row_to_batch(row)

    def list(self) -> list[model.Batch]:
        cursor = self._connection.execute(_get_select_batches_statement())
        result = []
        for row in cursor.all():
            result.append(_row_to_batch(row))
        return result
