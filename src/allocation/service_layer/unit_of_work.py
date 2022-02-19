import abc

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine, Connection
from sqlalchemy.orm import sessionmaker

from allocation import config
from allocation.adapters import repository

DEFAULT_ENGINE = create_engine(config.get_postgres_uri())


class AbstractUnitOfWork(abc.ABC):
    batches: repository.AbstractRepository

    def __exit__(self, *args):
        self.rollback()

    @abc.abstractmethod
    def commit(self):
        raise NotImplementedError

    @abc.abstractmethod
    def rollback(self):
        raise NotImplementedError


class SqlAlchemyUnitOfWork(AbstractUnitOfWork):

    def __init__(self, engine: Engine = DEFAULT_ENGINE):
        self.engine = engine

    def __enter__(self):
        self.connection: Connection = self.engine.begin().__enter__()
        self.transaction = self.connection.get_transaction()
        self.batches = repository.SqlAlchemyRepository(self.connection)

    def __exit__(self, *args):
        super().__exit__(*args)
        self.connection.close()

    def commit(self):
        self.transaction.commit()

    def rollback(self):
        self.transaction.rollback()
