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


class FakeRepository(repository.AbstractProductRepository):

    def __init__(self, products):
        self._products = set(products)

    def add(self, product: model.Product):
        self._products.add(product)

    def get(self, sku) -> model.Batch:
        return next((b for b in self._products if b.sku == sku), None)


class FakeUnitOfWork(AbstractUnitOfWork):

    def __init__(self):
        self.products = FakeRepository([])
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
    product = uow.products.get("CRUNCHY-ARMCHAIR")
    assert product._batches.pop().reference == 'b1'
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
    in_stock_batch = next(batch for batch in uow.products.get("RETRO-CLOCK")._batches
                          if batch.reference == "in-stock-batch")
    shipment_batch = next(batch for batch in uow.products.get("RETRO-CLOCK")._batches
                          if batch.reference == "shipment-batch")
    assert in_stock_batch.available_quantity == 90
    assert shipment_batch.available_quantity == 100


def test_prefers_earlier_batches():
    uow = FakeUnitOfWork()

    services.add_batch("speedy-batch", "MINIMALIST-SPOON", 100, today, uow)
    services.add_batch("normal-batch", "MINIMALIST-SPOON", 100, tomorrow, uow)
    services.add_batch("slow-batch", "MINIMALIST-SPOON", 100, later, uow)

    services.allocate("order1", "MINIMALIST-SPOON", 10, uow)

    speedy_batch = next(batch for batch in uow.products.get("MINIMALIST-SPOON")._batches
                        if batch.reference == "speedy-batch")
    normal_batch = next(batch for batch in uow.products.get("MINIMALIST-SPOON")._batches
                        if batch.reference == "normal-batch")
    slow_batch = next(batch for batch in uow.products.get("MINIMALIST-SPOON")._batches
                      if batch.reference == "slow-batch")

    assert speedy_batch.available_quantity == 90
    assert normal_batch.available_quantity == 100
    assert slow_batch.available_quantity == 100


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
