import abc
import functools
import typing as t
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

    join_stmt = batches. \
        join(allocations, batches.c.id == allocations.c.batch_id, isouter=True). \
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

    @property
    def connection(self) -> sa.engine.Connection:
        return self._connection

    def _create_batch(self, batch: model.Batch):
        stmt = insert(batches).values(
            reference=batch.reference,
            sku=batch.sku,
            eta=batch.eta,
            purchased_quantity=batch._purchased_quantity,
        ).returning(batches.c.id)
        row = self._connection.execute(stmt).one()
        return row.id

    def _get_order_lines_id(self, lines: t.Iterable[model.OrderLine]) -> t.Iterable[int]:
        clause = sa.or_(*[sa.and_(order_lines.c.orderid == line.orderid,
                                  order_lines.c.sku == line.sku) for line in lines])
        stmt = sa.select([order_lines.c.id]).where(clause)
        return (row.id for row in self._connection.execute(stmt).all())

    def _create_order_lines(self, lines: t.Iterable[model.OrderLine]):
        values = [dict(sku=line.sku, orderid=line.orderid, qty=line.qty) for line in lines]
        insert_stmt = insert(order_lines).values(values).on_conflict_do_nothing()
        self._connection.execute(insert_stmt)

    def _allocate(self, batch_id, lines):
        if not lines:
            return
        self._create_order_lines(lines)
        lines_id = self._get_order_lines_id(lines)
        insert_stmt = insert(allocations).values(
            [dict(orderline_id=line_id, batch_id=batch_id) for line_id in lines_id]
        ).on_conflict_do_nothing()
        self._connection.execute(insert_stmt)

    def _deallocate(self, batch_id, lines):
        if not lines:
            return
        lines_id = self._get_order_lines_id(lines)
        delete_stmt = sa.delete(allocations).where(
            allocations.c.batch_id == batch_id,
            allocations.c.orderline_id.in_(lines_id)
        )
        self._connection.execute(delete_stmt)

    def _batch_wrapper(self, batch):
        setattr(batch, '_storage__connection', self._connection)

    def add(self, batch: model.Batch):
        batch_id = getattr(batch, '_storage__id', None)
        if batch_id is None:
            batch_id = self._create_batch(batch)
        self._allocate(batch_id, batch._allocations)
        self._batch_wrapper(batch)
        object.__setattr__(batch, "_storage__id", batch_id)

    def get(self, reference) -> model.Batch:
        stmt = _get_select_batches_statement(batches.c.reference == reference)
        row = self._connection.execute(stmt).one()
        batch = _row_to_batch(row)
        self._batch_wrapper(batch)
        return batch

    def list(self) -> t.List[model.Batch]:
        cursor = self._connection.execute(_get_select_batches_statement())
        result = []
        for row in cursor.all():
            batch = _row_to_batch(row)
            self._batch_wrapper(batch)
            result.append(batch)
        return result


def start_mapper():

    def decorator(func, method):

        @functools.wraps(func)
        def wrapper(self: model.Batch, line: model.OrderLine):
            func(self, line)
            if hasattr(self, '_storage__connection'):
                storage = SqlAlchemyRepository(self._storage__connection)
                method(storage, self._storage__id, {line})

        return wrapper

    model.Batch.allocate = decorator(model.Batch.allocate, SqlAlchemyRepository._allocate)
    model.Batch.deallocate = decorator(model.Batch.deallocate, SqlAlchemyRepository._deallocate)


def clear_mapper():
    if hasattr(model.Batch.allocate, '__wrapped__'):
        model.Batch.allocate = model.Batch.allocate.__wrapped__
    if hasattr(model.Batch.deallocate, '__wrapped__'):
        model.Batch.deallocate = model.Batch.deallocate.__wrapped__
