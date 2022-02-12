import logging
from time import sleep

from batches.adapters import repository
from batches.adapters.repository import start_mapper
from batches.domain import model
import sqlalchemy as sa


def get_allocations(connection, batchid):
    rows = connection.execute(sa.text(
        "SELECT orderid"
        " FROM allocations"
        " JOIN order_lines ON allocations.orderline_id = order_lines.id"
        " JOIN batches ON allocations.batch_id = batches.id"
        " WHERE batches.reference = :batchid"
    ), batchid=batchid)
    return {row[0] for row in rows}


def test_repository_can_save_a_batch(session_factory):
    batch = model.Batch("batch1", 'RUSTY-SOAPDISH', 100, eta=None)
    repo = repository.SqlAlchemyRepository(session_factory())
    repo.add(batch)
    repo.connection.get_transaction().commit()

    connection = session_factory()
    rows = list(connection.execute(
        'SELECT reference, sku, purchased_quantity, eta FROM "batches"'
    ))
    assert rows == [("batch1", 'RUSTY-SOAPDISH', 100, None)]
    connection.get_transaction().commit()


def insert_order_line(connection):
    transaction = connection.begin()

    connection.execute(
        "INSERT INTO order_lines (orderid, sku, qty)"
        " VALUES ('order1', 'GENERIC-SOFA', 12)"
    )
    [[orderline_id]] = connection.execute(
        sa.text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        orderid="order1", sku="GENERIC-SOFA",
    )
    transaction.commit()
    return orderline_id


def insert_batch(connection, batch_id):
    connection.execute(
        sa.text("INSERT INTO batches (reference, sku, purchased_quantity, eta)"
                " VALUES (:batch_id, 'GENERIC-SOFA', 100, null)"),
        batch_id=batch_id,
    )
    [[batch_id]] = connection.execute(
        sa.text("SELECT id FROM batches WHERE reference=:batch_id AND sku='GENERIC-SOFA'"),
        batch_id=batch_id,
    )
    return batch_id


def insert_allocation(connection, orderline_id, batch_id):
    connection.execute(
        sa.text("INSERT INTO allocations (orderline_id, batch_id)"
                " VALUES (:orderline_id, :batch_id)"),
        orderline_id=orderline_id, batch_id=batch_id,
    )


def test_repository_can_retrieve_a_batch_with_allocations(session_factory):
    connection = session_factory()
    orderline_id = insert_order_line(connection)
    batch1_id = insert_batch(connection, "batch1")
    insert_batch(connection, "batch2")
    insert_allocation(connection, orderline_id, batch1_id)
    connection.get_transaction().commit()

    repo = repository.SqlAlchemyRepository(session_factory())
    retrieved = repo.get('batch1')
    expected = model.Batch('batch1', 'GENERIC-SOFA', 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12)
    }
    repo.connection.get_transaction().commit()


def test_updating_a_batch(session_factory):
    connection = session_factory()
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)
    repo = repository.SqlAlchemyRepository(connection)
    repo.add(batch)

    batch.allocate(order1)
    assert get_allocations(connection, "batch1") == {"order1"}

    batch.allocate(order2)
    assert get_allocations(connection, "batch1") == {"order1", "order2"}

    batch.deallocate(order1)
    assert get_allocations(connection, "batch1") == {"order2"}

    connection.get_transaction().commit()
