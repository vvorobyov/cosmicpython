from batches.adapters.repository import AbstractRepository
from batches.domain import model
from batches.domain.model import OrderLine


class InvalidSku(Exception):
    pass


def is_valid_sku(sku, batches):
    return sku in {b.sku for b in batches}


def allocate(line: OrderLine, repo: AbstractRepository, session) -> str:
    batches = repo.list()
    if not is_valid_sku(line.sku, batches):
        raise InvalidSku(f'Недопустимый артикул {line.sku}')
    batch = model.allocate(line, batches)
    repo.save(batch)
    session.get_transaction().commit()
    return batch.reference
