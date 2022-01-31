import pytest
import sqlalchemy as sa

from batches import config
from batches.adapters.db_tables import metadata


@pytest.fixture
def session():
    engine = sa.create_engine(config.get_postgres_uri())
    metadata.drop_all(engine)
    metadata.create_all(engine)
    yield engine
    metadata.drop_all(engine)
