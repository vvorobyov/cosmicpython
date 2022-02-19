import abc
import functools
import typing as t
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert

from allocation.domain import model

from .db_tables import batches, order_lines, allocations, products


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


class AbstractProductRepository(abc.ABC):
    @abc.abstractmethod
    def add(self, product: model.Product):
        pass

    @abc.abstractmethod
    def get(self, sku: str) -> model.Product:
        pass


class SqlAlchemyRepository(AbstractProductRepository):

    def __init__(self, connection: sa.engine.Connection):
        self.session = connection

    # def get(self, reference) -> t.Optional[model.Batch]:
    #     batch = next(self.select_batches(batches.c.reference == reference), None)
    #     if batch is None:
    #         return
    #     for _, line in self.select_lines(allocations.c.batch_id == batch.__repository_id__):
    #         batch._allocations.add(line)
    #     return batch

    def get(self, sku: str) -> t.Optional[model.Product]:
        if not self.check_product_exist(sku):
            return
        product = model.Product(sku, self.get_batches(sku))
        object.__setattr__(product, '__repository__', self)
        return product

    def add(self, product: model.Product):
        self.insert_product(product)

    @property
    def is_active(self) -> bool:
        return (not self.session.closed
                and self.session.get_transaction() is not None
                and self.session.get_transaction().is_active)

    def insert_product(self, product):
        insert_stmt = insert(products).values({'sku': product.sku})
        self.session.execute(insert_stmt)
        object.__setattr__(product, '__repository__', self)

    def check_product_exist(self, sku) -> bool:
        select_stmt = sa.select([products], products.c.sku == sku)
        result = self.session.execute(select_stmt).one_or_none()
        return bool(result)

    def get_batches(self, sku: str) -> list[model.Batch]:
        batches_dict: dict[int, model.Batch] = {
            batch.__repository_id__: batch
            for batch in self.select_batches(batches.c.sku == sku)}
        lines = self.select_lines(allocations.c.batch_id.in_(batches_dict.keys()))
        for batch_id, line in lines:
            batches_dict[batch_id]._allocations.add(line)
        return list(batches_dict.values())

    def insert_batch(self, batch: model.Batch) -> int:
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
        batch_id = next(self.select_batches(batches.c.reference == batch.reference)).__repository_id__
        object.__setattr__(batch, '__repository__', self)
        object.__setattr__(batch, '__repository_id__', batch_id)
        return batch_id

    def select_batches(self, *condition) -> t.Iterator[model.Batch]:
        batch_stmt = sa.select(batches)
        if condition:
            batch_stmt = batch_stmt.where(*condition)
        rows = self.session.execute(batch_stmt).all()
        for row in rows:
            batch = model.Batch(ref=row.reference, sku=row.sku, qty=row.purchased_quantity, eta=row.eta)
            object.__setattr__(batch, '__repository__', self)
            object.__setattr__(batch, '__repository_id__', row.id)
            yield batch

    def delete_allocations(self, batch_id: int, line_id: int):
        if self.is_active:
            delete_stmt = sa.delete(allocations).where(
                allocations.c.batch_id == batch_id,
                allocations.c.orderline_id == line_id
            )
            self.session.execute(delete_stmt)

    def insert_allocation(self, batch_id: int, line_id: int):
        if self.is_active:
            insert_stmt = insert(allocations).values({
                'batch_id': batch_id, 'orderline_id': line_id
            }).on_conflict_do_nothing()
            self.session.execute(insert_stmt)

    def select_lines(self, *condition) -> t.Iterator[tuple[int, model.OrderLine]]:
        """
        Метод получения строк заказа из базы
        :param condition:
        :return: id партии, строка заказа
        """
        join_stmt = (order_lines.
                     join(allocations, allocations.c.orderline_id == order_lines.c.id, isouter=True))

        lines_stmt = sa.select([allocations.c.batch_id,
                                order_lines]).select_from(join_stmt)
        if condition:
            lines_stmt = lines_stmt.where(*condition)
        rows = self.session.execute(lines_stmt).all()
        for row in rows:
            line = model.OrderLine(row.orderid, row.sku, row.qty)
            object.__setattr__(line, '__repository_id__', row.id)
            yield row.batch_id, line

    def sync_orderline(self, line: model.OrderLine) -> int:
        if hasattr(line, '__repository_id__'):
            return line.__repository_id__
        insert_stmt = insert(order_lines).values({
            'sku': line.sku,
            'orderid': line.orderid,
            'qty': line.qty
        }).on_conflict_do_nothing()
        self.session.execute(insert_stmt)
        [(_, stored_line)] = self.select_lines(
                order_lines.c.orderid == line.orderid,
                order_lines.c.sku == line.sku
            )
        return stored_line.__repository_id__


def activate():
    # Batch decorator
    def allocate_wrapper(func):
        def wrapper(batch: model.Batch, line: model.OrderLine):
            func(batch, line)
            if hasattr(batch, '__repository__') and line in batch._allocations:
                repository: SqlAlchemyRepository = batch.__repository__
                line_id = repository.sync_orderline(line)
                repository.insert_allocation(batch.__repository_id__, line_id)

        wrapper.__original__ = func
        return wrapper

    def deallocate_wrapper(func):
        def wrapper(batch: model.Batch, line: model.OrderLine):
            func(batch, line)
            print(f'deallocate {line=} from {batch=}')
            if hasattr(batch, '__repository__') and line not in batch._allocations:
                repository: SqlAlchemyRepository = batch.__repository__
                line_id = repository.sync_orderline(line)
                repository.delete_allocations(batch.__repository_id__, line_id)

        wrapper.__original__ = func
        return wrapper

    if not hasattr(model.Batch.allocate, '__original__'):
        model.Batch.allocate = allocate_wrapper(model.Batch.allocate)
    if not hasattr(model.Batch.deallocate, '__original__'):
        model.Batch.deallocate = deallocate_wrapper(model.Batch.deallocate)

    # Product decorate
    def add_batch_wrapper(func):
        def wrapper(product: model.Product, batch: model.Batch):
            func(product, batch)
            if hasattr(product, '__repository__') and batch in product._batches:
                repository: SqlAlchemyRepository = product.__repository__
                repository.insert_batch(batch)

        wrapper.__original__ = func
        return wrapper

    if not hasattr(model.Product.add_batch, '__original__'):
        model.Product.add_batch = add_batch_wrapper(model.Product.add_batch)


def clear():
    # clear Batch
    if hasattr(model.Batch.allocate, '__original__'):
        model.Batch.allocate = model.Batch.allocate.__original__

    if hasattr(model.Batch.deallocate, '__original__'):
        model.Batch.deallocate = model.Batch.deallocate.__original__

    if hasattr(model.Product.add_batch, '__original__'):
        model.Product.add_batch = model.Product.add_batch.__original__
