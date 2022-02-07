import typing as t
from datetime import date

from batches.adapters.repository import AbstractRepository
from batches.domain import model
from batches.domain.model import OrderLine


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def add_batch(
        reference: str, sku: str, qty: int, eta: t.Optional[date],
        repo: AbstractRepository, session,
):
    repo.save(model.Batch(reference, sku, qty, eta))
    session.get_transaction().commit()


def allocate(orderid: str, sku: str, qty: int,
             repo: AbstractRepository, session) -> str:
    batches = repo.list()
    line = OrderLine(orderid, sku, qty)
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f'Недопустимый артикул {line.sku}')
    batch = model.allocate(line, batches)
    repo.save(batch)
    session.get_transaction().commit()
    return batch.reference
