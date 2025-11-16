from contextlib import contextmanager
from typing import Generator

import duckdb

from domain import port


class StorageConnectionAdapter(port.IStorageConnectionAdapter):
    @classmethod
    def from_duckdb_memory(cls, env: dict) -> "StorageConnectionAdapter":
        db_file = env["db_file"]
        instance = cls()
        instance._db_file = db_file
        return instance

    def __init__(self):
        super().__init__()

    def get_connection(self):
        return duckdb.connect(self._db_file)

    def close_connection(self):
        self.get_connection().close()

    @contextmanager
    def connect(self):
        try:
            conn = self.get_connection()
            yield conn
        finally:
            conn.close()

    @contextmanager
    def create_transaction(self) -> Generator[port.IStorageSession, None, None]:
        try:
            conn = self.get_connection()
            with self.connect() as conn:
                conn.begin()
                yield conn
                conn.commit()
        except Exception as err:
            conn.rollback()
            raise err

    pass
