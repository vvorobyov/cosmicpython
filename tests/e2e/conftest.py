from pathlib import Path
import time
import sqlalchemy as sa
import pytest
import requests

from batches import config


def wait_for_webapp_to_come_up():
    deadline = time.time() + 10
    url = config.get_api_url()
    while time.time() < deadline:
        try:
            return requests.get(url, verify=False)
        except ConnectionError:
            time.sleep(0.5)
    pytest.fail("API never came up")


@pytest.fixture
def restart_api():
    time.sleep(0.5)
    wait_for_webapp_to_come_up()


@pytest.fixture
def add_stock(session):
    batches_added = set()
    skus_added = set()
    connection = session

    def _add_stock(lines):
        with connection.begin() as trn:
            for ref, sku, qty, eta in lines:
                connection.execute(sa.text(
                    "INSERT INTO batches (reference, sku, purchased_quantity, eta)"
                    " VALUES (:ref, :sku, :qty, :eta)"),
                    dict(ref=ref, sku=sku, qty=qty, eta=eta),
                )
                [[batch_id]] = connection.execute(sa.text(
                    "SELECT id FROM batches WHERE reference=:ref AND sku=:sku"),
                    dict(ref=ref, sku=sku),
                )
                batches_added.add(batch_id)
                skus_added.add(sku)
            trn.commit()

    yield _add_stock
    with connection.begin() as trn:
        for batch_id in batches_added:
            connection.execute(sa.text(
                "DELETE FROM allocations WHERE batch_id=:batch_id"),
                dict(batch_id=batch_id),
            )
            connection.execute(sa.text(
                "DELETE FROM batches WHERE id=:batch_id"),
                dict(batch_id=batch_id),
            )
        for sku in skus_added:
            connection.execute(sa.text(
                "DELETE FROM order_lines WHERE sku=:sku"),
                dict(sku=sku),
            )
        trn.commit()
