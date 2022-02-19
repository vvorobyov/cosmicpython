from datetime import date, timedelta

import pytest

from allocation.adapters import repository
from allocation.domain import model
from allocation.domain.model import OutOfStock
from allocation.service_layer import services
from allocation.service_layer.unit_of_work import AbstractUnitOfWork

today = date.today()
tomorrow = today + timedelta(days=1)
later = tomorrow + timedelta(days=7)


class FakeRepository(repository.AbstractRepository):

    def __init__(self, batches):
        self._batches = set(batches)

    def add(self, batch: model.Batch):
        self._batches.add(batch)

    def get(self, reference) -> model.Batch:
        return next(b for b in self._batches if b.reference == reference)

    def list(self) -> list[model.Batch]:
        return list(self._batches)


class FakeUnitOfWork(AbstractUnitOfWork):

    def __init__(self):
        self.batches = FakeRepository([])
        self.committed = False

    def __enter__(self):
        pass

    def commit(self):
        self.committed = True

    def rollback(self):
        pass


def test_add_batch():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "CRUNCHY-ARMCHAIR", 100, None, uow)
    assert uow.batches.get('b1') is not None
    assert uow.committed


def test_returns_allocation():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "COMPLICATED-LAMP", 100, None, uow)
    result = services.allocate("o1", "COMPLICATED-LAMP", 10, uow)
    assert result == "b1"


def test_error_for_invalid_sku():
    uow = FakeUnitOfWork()
    services.add_batch("b1", "AREALSKU", 100, None, uow)

    with pytest.raises(services.InvalidSku, match="Недопустимый артикул NONEXISTENTSKU"):
        services.allocate("o1", "NONEXISTENTSKU", 10, uow)


def test_commits():
    uow = FakeUnitOfWork()

    services.add_batch("b1", "OMINOUS-MIRROR", 100, None, uow)
    services.allocate("o1", "OMINOUS-MIRROR", 10, uow)
    assert uow.committed is True


def test_prefers_warehouse_batches_to_shipments():
    uow = FakeUnitOfWork()
    services.add_batch("in-stock-batch", "RETRO-CLOCK", 100, None, uow)
    services.add_batch("shipment-batch", "RETRO-CLOCK", 100, tomorrow, uow)

    services.allocate("oref", "RETRO-CLOCK", 10, uow)
    assert uow.batches.get("in-stock-batch").available_quantity == 90
    assert uow.batches.get("shipment-batch").available_quantity == 100


def test_prefers_earlier_batches():
    uow = FakeUnitOfWork()

    services.add_batch("speedy-batch", "MINIMALIST-SPOON", 100, today, uow)
    services.add_batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow, uow)
    services.add_batch("slow-batch", "MINIMALIST-SPOON", 100, later, uow)

    services.allocate("order1", "MINIMALIST-SPOON", 10, uow)
    assert uow.batches.get("speedy-batch").available_quantity == 90
    assert uow.batches.get("normal-batch").available_quantity == 100
    assert uow.batches.get("slow-batch").available_quantity == 100


def test_raises_out_of_stock_exception_if_cannot_allocate():
    uow = FakeUnitOfWork()
    services.add_batch('batch1', 'SMALL-FORK', 10, today, uow)

    services.allocate('order1', 'SMALL-FORK', 10, uow)
    with pytest.raises(OutOfStock, match='SMALL-FORK'):
        services.allocate('order2', 'SMALL-FORK', 1, uow)


def test_returns_allocated_batch_ref():
    uow = FakeUnitOfWork()
    services.add_batch("in-stock-batch-ref", "HIGHBROW-POSTER", 100, None, uow)
    services.add_batch("shipment-batch-ref", "HIGHBROW-POSTER", 100, tomorrow, uow)

    batchref = services.allocate("oref", "HIGHBROW-POSTER", 10, uow)
    assert batchref == "in-stock-batch-ref"
