from sqlalchemy.engine import RootTransaction

from batches.adapters import repository
from batches.domain import model
import sqlalchemy as sa


def get_allocations(session, batchid):
    connection = session.connect()
    rows = list(
        connection.execute(
            sa.text("SELECT orderid"
                    " FROM allocations"
                    " JOIN order_lines ON allocations.orderline_id = order_lines.id"
                    " JOIN batches ON allocations.batch_id = batches.id"
                    " WHERE batches.reference = :batchid"),
            batchid=batchid,
        )
    )
    connection.close()
    return {row[0] for row in rows}


def test_repository_can_save_a_batch(session):
    batch = model.Batch("batch1", 'RUSTY-SOAPDISH', 100, eta=None)

    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)
    repo.commit()
    conn = session.connect()
    rows = list(conn.execute(
        'SELECT reference, sku, purchased_quantity, eta FROM "batches"'
    ))
    conn.close()
    assert rows == [("batch1", 'RUSTY-SOAPDISH', 100, None)]


def insert_order_line(session):
    connection = session.connect()
    transaction = connection.begin()

    connection.execute(
        "INSERT INTO order_lines (orderid, sku, qty)"
        " VALUES ('order1', 'GENERIC-SOFA', 12)"
    )
    [[orderline_id]] = transaction.connection.execute(
        sa.text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        orderid="order1", sku="GENERIC-SOFA",
    )
    transaction.commit()
    connection.close()
    return orderline_id


def insert_batch(session, batch_id):
    connection = session.connect()
    transaction = connection.begin()
    connection.execute(
        sa.text("INSERT INTO batches (reference, sku, purchased_quantity, eta)"
                " VALUES (:batch_id, 'GENERIC-SOFA', 100, null)"),
        batch_id=batch_id,
    )
    [[batch_id]] = transaction.connection.execute(
        sa.text("SELECT id FROM batches WHERE reference=:batch_id AND sku='GENERIC-SOFA'"),
        batch_id=batch_id,
    )
    transaction.commit()
    connection.close()
    return batch_id


def insert_allocation(session, orderline_id, batch_id):
    connection = session.connect()
    transaction = connection.begin()
    connection.execute(
        sa.text("INSERT INTO allocations (orderline_id, batch_id)"
                " VALUES (:orderline_id, :batch_id)"),
        orderline_id=orderline_id, batch_id=batch_id,
    )
    transaction.commit()
    connection.close()


def test_repository_can_retrieve_a_batch_with_allocations(session):
    orderline_id = insert_order_line(session)
    batch1_id = insert_batch(session, "batch1")
    insert_batch(session, "batch2")
    insert_allocation(session, orderline_id, batch1_id)

    repo = repository.SqlAlchemyRepository(session)
    retrieved = repo.get('batch1')
    expected = model.Batch('batch1', 'GENERIC-SOFA', 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12)
    }


def test_updating_a_batch(session):
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)
    batch.allocate(order1)

    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)
    repo.commit()

    repo = repository.SqlAlchemyRepository(session)
    batch.allocate(order2)
    repo.add(batch)
    repo.commit()

    assert get_allocations(session, "batch1") == {"order1", "order2"}

    batch.deallocate(order1)
    repo = repository.SqlAlchemyRepository(session)
    repo.add(batch)
    repo.commit()
    assert get_allocations(session, "batch1") == {"order2"}
