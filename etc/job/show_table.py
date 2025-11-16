
from config import loader
env = loader.load_env(['./config/root.local.yml'])

from infrastructure import storage
from infrastructure import repository
dm = storage.StorageConnectionAdapter.from_duckdb_memory(env['storage'])

from fastapi.testclient import TestClient
from interfaces import fastapi as fa
api = fa.setup_fastapi()
client = TestClient(api)

# Schema constante para reutilização
def schema_creator(namespace: str):
    return {
    "type": "record",
    "namespace": namespace,
    "name": "RegistroUsuario",
    "fields": [
        {"name": "name", "type": "string"},
        {"name": "age", "type": "int"},
        {"name": "salary", "type": "double"},
        {"name": "data_criacao", "type": "string"},
        {"name": "data_nascimento", "type": "string"},
        {"name": "hora_registro", "type": "string"},
        {"name": "tags", "type": {"type": "array", "items": "string"}},
        {"name": "codigo", "type": ["null", "int"], "default": None},
    ],
}


client.put('/schema', json=schema_creator('rfb.json'))


result = dm.get_connection().execute("select * from schema_registry").fetchall()
print(result)