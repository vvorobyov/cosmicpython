import pytest
import sqlalchemy as sa

from allocation import config
from allocation.adapters.db_tables import metadata


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
    yield Session(engine)
