import json
from infrastructure import storage
from infrastructure import repository
from infrastructure import broker
from etc.config import loader
from infrastructure import bucket, storage
# Inicializa o BucketManager
env = loader.load_env(['./etc/config/root.local.yml'])


bm = bucket.BucketAdapter.from_minio_client(env['bucket'])
dm = storage.StorageConnectionAdapter.from_duckdb_memory(env['storage'])
ds = repository.SchemaRegistry()
br = broker.BrokerAdapter(env['broker'])
br.publish_message(env['app']['source_router'], json.dumps({'namespace' : 'rfb.json'}, ensure_ascii=False))

with dm.create_transaction() as conn:
    ds.initialize_schema(conn)
