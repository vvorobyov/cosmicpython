import pytest
import sqlalchemy as sa

from batches import config
from batches.adapters.db_tables import metadata
from batches.adapters.repository import start_mapper, clear_mapper


@pytest.fixture(name='engine')
def engine_factory():
    engine = sa.create_engine(config.get_postgres_uri())
    metadata.drop_all(engine)
    metadata.create_all(engine)
    yield engine
    metadata.drop_all(engine)


@pytest.fixture
def session_factory(engine):
    class Session:
        def __init__(self, engine):
            self.engine = engine

        def __call__(self):
            return self.engine.begin().__enter__()
    start_mapper()
    yield Session(engine)
    clear_mapper()
