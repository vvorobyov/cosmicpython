
from allocation.adapters import repository
from allocation.domain import model
import sqlalchemy as sa


def insert_order_line(connection, orderid: str = 'order1'):
    transaction = connection.begin()

    connection.execute(
        sa.text("INSERT INTO order_lines (orderid, sku, qty)"
        " VALUES (:orderid, 'GENERIC-SOFA', 12)"),
        orderid=orderid
    )
    [[orderline_id]] = connection.execute(
        sa.text("SELECT id FROM order_lines WHERE orderid=:orderid AND sku=:sku"),
        orderid="order1", sku="GENERIC-SOFA",
    )
    transaction.commit()
    return orderline_id


def insert_batch(connection, batch_id):
    connection.execute(
        sa.text("INSERT INTO allocation (reference, sku, purchased_quantity, eta)"
                " VALUES (:batch_id, 'GENERIC-SOFA', 100, null)"),
        batch_id=batch_id,
    )
    [[batch_id]] = connection.execute(
        sa.text("SELECT id FROM allocation WHERE reference=:batch_id AND sku='GENERIC-SOFA'"),
        batch_id=batch_id,
    )
    return batch_id


def insert_allocation(connection, orderline_id, batch_id):
    connection.execute(
        sa.text("INSERT INTO allocations (orderline_id, batch_id)"
                " VALUES (:orderline_id, :batch_id)"),
        orderline_id=orderline_id, batch_id=batch_id,
    )


def test_repository_can_retrieve_all_batch(session_factory):
    connection = session_factory()
    orderline1_id = insert_order_line(connection)
    orderline2_id = insert_order_line(connection, "order2")
    batch1_id = insert_batch(connection, "batch1")
    batch2_id = insert_batch(connection, "batch2")
    insert_allocation(connection, orderline1_id, batch1_id)
    insert_allocation(connection, orderline2_id, batch2_id)
    connection.get_transaction().commit()
    repo = repository.SqlAlchemyRepository(session_factory())
    batches = repo.list_batches()
    assert len(batches) == 2
    batch1 = next(batch for batch in batches if batch.reference == "batch1")
    assert batch1._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12)
    }
    connection.close()
    repo.session.close()



def test_repository_can_retrieve_a_batch_with_allocations(session_factory):
    connection = session_factory()
    orderline_id = insert_order_line(connection)
    batch1_id = insert_batch(connection, "batch1")
    insert_batch(connection, "batch2")
    insert_allocation(connection, orderline_id, batch1_id)
    connection.get_transaction().commit()

    repo = repository.SqlAlchemyRepository(session_factory())
    retrieved = repo.get_batch('batch1')
    expected = model.Batch('batch1', 'GENERIC-SOFA', 100, eta=None)
    assert retrieved == expected
    assert retrieved.sku == expected.sku
    assert retrieved._purchased_quantity == expected._purchased_quantity
    assert retrieved._allocations == {
        model.OrderLine("order1", "GENERIC-SOFA", 12)
    }
    repo.session.get_transaction().commit()
    repo.session.close()


def get_allocations(connection, batchid):
    rows = connection.execute(sa.text(
        "SELECT orderid"
        " FROM allocations"
        " JOIN order_lines ON allocations.orderline_id = order_lines.id"
        " JOIN allocation ON allocations.batch_id = allocation.id"
        " WHERE allocation.reference = :batchid"
    ), batchid=batchid)
    return {row[0] for row in rows}


def test_repository_can_save_a_batch(session_factory):
    batch = model.Batch("batch1", 'RUSTY-SOAPDISH', 100, eta=None)
    repo = repository.SqlAlchemyRepository(session_factory())
    repo.save(batch)
    repo.session.get_transaction().commit()

    connection = session_factory()
    rows = list(connection.execute(
        'SELECT reference, sku, purchased_quantity, eta FROM "allocation"'
    ))
    assert rows == [("batch1", 'RUSTY-SOAPDISH', 100, None)]
    connection.get_transaction().commit()


def test_updating_a_batch(session_factory):
    connection = session_factory()
    order1 = model.OrderLine("order1", "WEATHERED-BENCH", 10)
    order2 = model.OrderLine("order2", "WEATHERED-BENCH", 20)
    batch = model.Batch("batch1", "WEATHERED-BENCH", 100, eta=None)
    repo = repository.SqlAlchemyRepository(connection)

    batch.allocate(order1)
    repo.save(batch)
    assert get_allocations(connection, "batch1") == {"order1"}

    batch.allocate(order2)
    repo.save(batch)
    assert get_allocations(connection, "batch1") == {"order1", "order2"}

    batch.deallocate(order1)
    repo.save(batch)
    assert get_allocations(connection, "batch1") == {"order2"}

    connection.get_transaction().commit()
