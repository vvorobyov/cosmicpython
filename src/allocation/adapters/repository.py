import abc
import functools
import typing as t
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from allocation.domain import model

from .db_tables import batches, order_lines, allocations


class AbstractRepository(abc.ABC):

    @abc.abstractmethod
    def save(self, batch: model.Batch):
        raise NotImplementedError

    @abc.abstractmethod
    def get_batch(self, reference) -> model.Batch:
        raise NotImplementedError

    @abc.abstractmethod
    def list_batches(self) -> list[model.Batch]:
        raise NotImplementedError


class SqlAlchemyRepository(AbstractRepository):

    def __init__(self, connection: sa.engine.Connection):
        self.session = connection

    def save(self, batch: model.Batch):
        batch_id = self.save_batch(batch)

        stored = list(self.extract_lines(allocations.c.batch_id == batch_id))
        added_lines = batch._allocations - {line for _, _, line in stored}  # noqa
        removed_lines = {id_ for _, id_, line in stored if line not in batch._allocations}  # noqa
        added_ids = self.save_orderlines(added_lines)
        self.allocate_lines(batch_id, added_ids)
        self.deallocate_lines(batch_id, removed_lines)

    def save_batch(self, batch: model.Batch) -> int:
        """
        Метод сохранения партии, без аллокаций
        :param batch: Партия
        :return: id партии
        """
        insert_stmt = insert(batches).values({
            'reference': batch.reference,
            'sku': batch.sku,
            'purchased_quantity': batch._purchased_quantity,  # noqa
            'eta': batch.eta
        }).on_conflict_do_nothing()
        self.session.execute(insert_stmt)
        [[batch_id]] = self.session.execute(
            sa.select([batches.c.id]).where(batches.c.reference == batch.reference))
        return batch_id

    def extract_batches(self, *condition) -> t.Iterator[tuple[int, model.Batch]]:
        batch_stmt = sa.select(batches)
        if condition:
            batch_stmt = batch_stmt.where(*condition)
        rows = self.session.execute(batch_stmt).all()
        return ((row.id, model.Batch(ref=row.reference, sku=row.sku, qty=row.purchased_quantity, eta=row.eta))
                for row in rows)

    def save_orderlines(self, lines: t.Iterable[model.OrderLine]) -> t.Iterator[int]:
        """
        Метод сохранения строк заказав
        :param lines: Строки заказов
        :return: список
        """
        if not lines:
            return ()
        insert_stmt = insert(order_lines).values([
            {'sku': line.sku,
             'orderid': line.orderid,
             'qty': line.qty}
            for line in lines
        ]).on_conflict_do_nothing()
        self.session.execute(insert_stmt)
        select_stmt = sa.select([order_lines.c.id]).where(
            sa.or_(sa.and_(
                order_lines.c.orderid == line.orderid, order_lines.c.sku == line.sku
            ) for line in lines))
        rows = self.session.execute(select_stmt)
        return (row.id for row in rows)

    def deallocate_lines(self, batch_id: int, lines_ids: t.Iterable[int]):
        if not lines_ids:
            return
        delete_stmt = sa.delete(allocations).where(
            allocations.c.batch_id == batch_id,
            allocations.c.orderline_id.in_(lines_ids)
        )
        self.session.execute(delete_stmt)

    def allocate_lines(self, batch_id: int, lines_ids: t.Iterable[int]):
        if not lines_ids:
            return
        insert_stmt = insert(allocations).values([
            {'batch_id': batch_id, 'orderline_id': line_id} for line_id in lines_ids
        ])
        self.session.execute(insert_stmt)

    def extract_lines(self, *condition) -> t.Iterator[tuple[int, int, model.OrderLine]]:
        """
        Метод получения строк заказа из базы
        :param condition:
        :return: id партии, id заказа, строка заказа
        """
        join_stmt = (order_lines.
                     join(allocations, allocations.c.orderline_id == order_lines.c.id, isouter=True))

        lines_stmt = sa.select([allocations.c.batch_id,
                                order_lines]).select_from(join_stmt)
        if condition:
            lines_stmt = lines_stmt.where(*condition)
        rows = self.session.execute(lines_stmt).all()
        return ((row.batch_id, row.id, model.OrderLine(row.orderid, row.sku, row.qty))
                for row in rows)

    def get_batch(self, reference) -> t.Optional[model.Batch]:
        _, batch = next(self.extract_batches(batches.c.reference == reference), None)
        if batch is None:
            return
        for batch_id, line_id, line in self.extract_lines(batches.c.reference == reference):
            batch.allocate(line)
        return batch

    def list_batches(self) -> list[model.Batch]:
        batches_dict: dict[int, model.Batch] = {
            batch_id: batch
            for batch_id, batch in self.extract_batches()}
        lines = self.extract_lines(allocations.c.batch_id.in_(batches_dict.keys()))
        for batch_id, line_id, line in lines:
            batches_dict[batch_id].allocate(line)
        return list(batches_dict.values())

