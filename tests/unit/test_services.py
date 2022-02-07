from datetime import date, timedelta

import pytest

from batches.adapters import repository
from batches.domain.model import OutOfStock
from batches.service_layer import services

today = date.today()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(days=7)


class FakeRepository(repository.AbstractRepository):

    def __init__(self, batches):
        self._batches = set(batches)

    def save(self, batch):
        self._batches.add(batch)

    def get(self, reference):
        return next(b for b in self._batches if b.reference == reference)

    def list(self):
        return list(self._batches)


class Transaction:
    committed = False

    def commit(self):
        self.committed = True


class FakeSession:
    def __init__(self):
        self._transaction = Transaction()

    def get_transaction(self):
        return self._transaction


def test_add_batch():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, repo, session)
    assert repo.get('b1') is not None
    assert session.get_transaction().committed


def test_returns_allocation():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "COMPLICATED-LAMP", 100, None, repo, session)

    result = services.allocate("o1", "COMPLICATED-LAMP", 10, repo, FakeSession())
    assert result == "b1"


def test_error_for_invalid_sku():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "AREALSKU", 100, None, repo, session)

    with pytest.raises(services.InvalidSku, match="Недопустимый артикул NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, repo, FakeSession())


def test_commits():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("b1", "OMINOUS-MIRROR", 100, None, repo, session)

    services.allocate("o1", "OMINOUS-MIRROR", 10, repo, session)
    assert session.get_transaction().committed is True


def test_prefers_warehouse_batches_to_shipments():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("in-stock-batch", "RETRO-CLOCK", 100, None, repo, session)
    services.add_batch("shipment-batch", "RETRO-CLOCK", 100, tomorrow, repo, session)

    services.allocate("oref", "RETRO-CLOCK", 10, repo, session)
    assert repo.get("in-stock-batch").available_quantity == 90
    assert repo.get("shipment-batch").available_quantity == 100


def test_prefers_earlier_batches():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("speedy-batch", "MINIMALIST-SPOON", 100, today, repo, session)
    services.add_batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow, repo, session)
    services.add_batch("slow-batch", "MINIMALIST-SPOON", 100, later, repo, session)

    services.allocate("order1", "MINIMALIST-SPOON", 10, repo, session)
    assert repo.get("speedy-batch").available_quantity == 90
    assert repo.get("normal-batch").available_quantity == 100
    assert repo.get("slow-batch").available_quantity == 100


def test_raises_out_of_stock_exception_if_cannot_allocate():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch('batch1', 'SMALL-FORK', 10, today, repo, session)

    services.allocate('order1', 'SMALL-FORK', 10, repo, session)
    with pytest.raises(OutOfStock, match='SMALL-FORK'):
        services.allocate('order2', 'SMALL-FORK', 1, repo, session)


def test_returns_allocated_batch_ref():
    repo, session = FakeRepository([]), FakeSession()
    services.add_batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None, repo, session)
    services.add_batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow, repo, session)

    batchref = services.allocate("oref", "HIGHBROW-POSTER", 10, repo, session)
    assert batchref == "in-stock-batch-ref"
