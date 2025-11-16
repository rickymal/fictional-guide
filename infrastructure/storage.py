from domain import port
from contextlib import contextmanager
import duckdb
from typing import Generator
import os
class StorageConnectionAdapter(port.IStorageConnectionAdapter):
    def __init__(self):
        super().__init__()
        self._db_file = None

    @classmethod
    def from_duckdb_memory(cls, env: dict) -> 'StorageConnectionAdapter':
        instance = cls()
        db_file = env.get('db_file', 'main.duckdb')
        
        data_dir = '/data'
        instance._db_file = os.path.join(data_dir, db_file)
        
        # Garantir que o diretório existe
        os.makedirs(os.path.dirname(instance._db_file), exist_ok=True)
        
        return instance

    def get_connection(self):
        if self._db_file is None:
            raise ValueError("db_file não foi definido")
        return duckdb.connect(self._db_file)
    
    def close_connection(self):
        pass

    @contextmanager
    def connect(self):
        conn = self.get_connection()
        try:
            yield conn
        finally:
            conn.close()
            
    @contextmanager
    def create_transaction(self) -> Generator[port.IStorageSession, None, None]:
        with self.connect() as conn:
            try:
                conn.execute("BEGIN TRANSACTION")
                yield conn
                conn.execute("COMMIT")
            except Exception as err:
                conn.execute("ROLLBACK")
                raise err