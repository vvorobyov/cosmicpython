import typing as t
from datetime import date

from allocation.adapters.repository import AbstractRepository
from allocation.domain import model
from allocation.domain.model import OrderLine
from allocation.service_layer.unit_of_work import AbstractUnitOfWork


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
        reference: str, sku: str, qty: int, eta: t.Optional[date],
        uow: AbstractUnitOfWork
):
    with uow:
        product = uow.products.get(sku)
        if product is None:
            product = model.Product(sku, batches=[])
            uow.products.add(product)
        product.add_batch(model.Batch(reference, sku, qty, eta))
        uow.commit()


def allocate(orderid: str, sku: str, qty: int,
             uow: AbstractUnitOfWork) -> str:
    line = OrderLine(orderid, sku, qty)
    with uow:
        product = uow.products.get(sku)
        if product is None:
            raise InvalidSku(f'Недопустимый артикул {line.sku}')
        batchref = product.allocate(line)
        uow.commit()
    return batchref
