import glob
import json
import uuid

from config import loader

from infrastructure import bucket, storage

# Inicializa o BucketManager
env = loader.load_env(["./etc/config/root.local.yml"])
bm = bucket.BucketAdapter.from_minio_client(env["bucket"])


# Reinicia os buckets para garantir um ambiente limpo
bm.remove_bucket_if_exists("gold")
bm.create_bucket("gold")
bm.remove_bucket_if_exists("validated")
bm.create_bucket("validated")
bm.remove_bucket_if_exists("quarantine")
bm.create_bucket("quarantine")
