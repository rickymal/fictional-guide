
from config import loader
from infrastructure.storage import StorageConnectionAdapter
from infrastructure import repository
env = loader.load_env(['./config/root.local.yml'])
storage_connection = StorageConnectionAdapter.from_duckdb_memory(env['storage'])
from domain import constants


with storage_connection.connect() as conn:
    repository.QueryWriter.run_sql_in_file(conn, env['app']['migration'], [])