import pytest
import sqlalchemy as sa

from batches import config
from batches.adapters.db_tables import metadata


@pytest.fixture
def connection():
    engine = sa.create_engine(config.get_postgres_uri())
    metadata.drop_all(engine)
    metadata.create_all(engine)
    conn = engine.connect()
    yield conn
    conn.close()
    metadata.drop_all(engine)
