import pytest
import sqlalchemy as sa

from batches import config
from batches.adapters.db_tables import metadata


@pytest.fixture
def session() -> sa.engine.Connection:
    engine = sa.create_engine(config.get_postgres_uri())
    metadata.drop_all(engine)
    metadata.create_all(engine)
    connection = engine.connect()
    yield connection
    connection.close()
    metadata.drop_all(engine)
