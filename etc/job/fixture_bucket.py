import glob
import json
import uuid

from etc.config import loader

from infrastructure import bucket, storage

# Inicializa o BucketManager
env = loader.load_env(["./etc/config/root.local.yml"])
bm = bucket.BucketAdapter.from_minio_client(env["bucket"])


# Reinicia os buckets para garantir um ambiente limpo
bm.remove_bucket_if_exists(env['app']['source_bucket'])
bm.create_bucket(env['app']['source_bucket'])
bm.remove_bucket_if_exists(env['app']['validate_bucket'])
bm.create_bucket(env['app']['validate_bucket'])
bm.remove_bucket_if_exists(env['app']['quarantine_bucket'])
bm.create_bucket(env['app']['quarantine_bucket'])
