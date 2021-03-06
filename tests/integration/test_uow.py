import threading
import time
import traceback

import pytest
import sqlalchemy as sa
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from allocation import config
from allocation.domain import model
from allocation.service_layer import unit_of_work
from random_refs import random_sku, random_batchref, random_orderid


def insert_product(connection, sku='GENERIC-SOFA', product_version=1):
    connection.execute(
        sa.text("INSERT INTO products (sku)"
                " VALUES (:sku)"),
        sku=sku
    )


def insert_batch(connection, ref, sku, qty, eta):
    connection.execute(
        sa.text("INSERT INTO batches (reference, sku, purchased_quantity, eta)"
                " VALUES (:ref, :sku, :qty, :eta)"),
        dict(ref=ref, sku=sku, qty=qty, eta=eta, )
    )


def get_allocated_batch_ref(session, orderid, sku):
    [[orderlineid]] = session.execute(
        sa.text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        orderid=orderid, sku=sku,
    )
    [[batchref]] = session.execute(
        sa.text("SELECT b.reference FROM allocations JOIN batches AS b ON batch_id = b.id"
                " WHERE orderline_id=:orderlineid"),
        orderlineid=orderlineid,
    ).all()
    return batchref


def test_uow_can_retrieve_a_batch_and_allocate_to_it(engine):
    uof = unit_of_work.SqlAlchemyUnitOfWork(engine)
    with uof:
        insert_product(uof.connection, 'HIPSTER-WORKBENCH')
        insert_batch(uof.connection, "batch1", "HIPSTER-WORKBENCH", 100, None)
        uof.commit()

    uow = unit_of_work.SqlAlchemyUnitOfWork(engine)
    with uow:
        product = uow.products.get('HIPSTER-WORKBENCH')
        line = model.OrderLine('o1', 'HIPSTER-WORKBENCH', 10)
        product.allocate(line)
        uow.commit()

    uof = unit_of_work.SqlAlchemyUnitOfWork(engine)
    with uof:
        batchref = get_allocated_batch_ref(uof.connection, "o1", "HIPSTER-WORKBENCH")
    assert batchref == "batch1"


# uncomment and fix these when ready
def test_rolls_back_uncommitted_work_by_default(engine):
    uow = unit_of_work.SqlAlchemyUnitOfWork(engine)
    with uow:
        insert_product(uow.connection, "MEDIUM-PLINTH")
        insert_batch(uow.connection, "batch1", "MEDIUM-PLINTH", 100, None)
    with uow:
        rows = list(uow.connection.execute('SELECT * FROM batches'))
    assert rows == []


def test_rolls_back_on_error(engine):
    class MyException(Exception):
        pass

    uow = unit_of_work.SqlAlchemyUnitOfWork(engine)
    with pytest.raises(MyException):
        with uow:
            insert_product(uow.connection, 'LARGE-FORK')
            insert_batch(uow.connection, "batch1", "LARGE-FORK", 100, None)
            raise MyException()
    with uow:
        rows = list(uow.connection.execute('SELECT * FROM batches'))
    assert rows == []


def try_to_allocate(orderid, sku, exceptions):
    line = model.OrderLine(orderid, sku, 10)
    try:
        with unit_of_work.SqlAlchemyUnitOfWork() as uow:
            product = uow.products.get(sku=sku)
            product.allocate(line)
            time.sleep(0.2)
            uow.commit()
    except Exception as e:
        exceptions.append(e)


def test_concurrent_updates_to_version_are_not_allowed(engine):
    sku, batch = random_sku(), random_batchref()
    session = engine.begin().__enter__()
    insert_product(session, sku)
    insert_batch(session, batch, sku, 100, eta=None)
    session.get_transaction().commit()

    order1, order2 = random_orderid(1), random_orderid(2)
    exceptions = []
    try_to_allocate_order1 = lambda: try_to_allocate(order1, sku, exceptions)
    try_to_allocate_order2 = lambda: try_to_allocate(order2, sku, exceptions)
    thread1 = threading.Thread(target=try_to_allocate_order1)
    thread2 = threading.Thread(target=try_to_allocate_order2)
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    session = engine.connect().__enter__()
    [exception] = exceptions
    assert '???? ???????????????????? ?????????????????????????? ???????????? ????-???? ???????????????????????? ????????????????????' in str(exception)

    orders = list(session.execute(
        sa.text("""
        select orderid from allocations
        join batches on allocations.batch_id = batches.id
        join order_lines on allocations.orderline_id = order_lines.id
        where order_lines.sku = :sku
        """),
        sku=sku
    ))

    session.close()
    assert len(orders) == 1
    with unit_of_work.SqlAlchemyUnitOfWork() as uow:
        uow.connection.execute('select 1')




