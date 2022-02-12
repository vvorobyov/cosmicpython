import typing as t
from datetime import date

from batches.adapters.repository import AbstractRepository
from batches.domain import model
from batches.domain.model import OrderLine
from batches.service_layer.unit_of_work import AbstractUnitOfWork


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
        reference: str, sku: str, qty: int, eta: t.Optional[date],
        uow: AbstractUnitOfWork
):
    with uow:
        uow.batches.add(model.Batch(reference, sku, qty, eta))
        uow.commit()


def allocate(orderid: str, sku: str, qty: int,
             uow: AbstractUnitOfWork) -> str:
    with uow:
        batches = uow.batches.list()
        line = OrderLine(orderid, sku, qty)
        if not is_valid_sku(line.sku, batches):
            raise InvalidSku(f'Недопустимый артикул {line.sku}')
        batch = model.allocate(line, batches)
        uow.commit()
    return batch.reference
