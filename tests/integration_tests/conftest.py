import pytest
import sqlalchemy as sa
from batches.adapters.db_tables import metadata


@pytest.fixture
def session():
    engine: sa.engine.Engine = sa.create_engine('postgresql://cosmic:example@localhost:30000/cosmic_db')
    metadata.drop_all(engine)
    metadata.create_all(engine)
    connection = engine.begin()
    yield connection
    connection.conn.close()
    metadata.drop_all(engine)
