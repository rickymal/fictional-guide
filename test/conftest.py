import pytest
from etc.config import loader
from fastapi.testclient import TestClient


@pytest.fixture(scope="session")
def test_client():
    from interfaces import fastapi

    api = fastapi.setup_fastapi()
    client = TestClient(api)
    return client


@pytest.fixture(scope="session")
def env():
    return loader.load_env(["etc/config/root.local.yml"])


@pytest.fixture(scope="session")
def bm(env: dict):
    from infrastructure import bucket

    bm = bucket.BucketAdapter.from_minio_client(env["bucket"])
    return bm


@pytest.fixture(scope="session")
def rm(env: dict):
    from infrastructure import broker

    rm = broker.BrokerAdapter(env["broker"])
    return rm


@pytest.fixture(scope="function")
def storage(env: dict):
    from infrastructure.storage import StorageConnectionAdapter

    storage_connection = StorageConnectionAdapter.from_duckdb_memory(env["storage"])
    yield storage_connection
    storage_connection.close_connection()
