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


