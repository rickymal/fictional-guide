from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, List, Dict, Union, IO, Iterator, Generator
from datetime import datetime, timedelta

class IBucketAdapter(ABC):
    @abstractmethod
    def __init__(self, client: object):
        ...

    @classmethod
    @abstractmethod
    def from_minio_client(cls, env: dict):
        ...

    @abstractmethod
    def delete_object(self, bucket_name: str, object_name: str) -> bool:
        ...

    @abstractmethod
    def create_bucket(self, bucket_name: str) -> bool:
        ...

    @abstractmethod
    def put_object(self, bucket_name: str, object_name: str, data: object, content_type: str):
        ...

    @abstractmethod
    def iter_bucket_by_prefix_key(self, bucket_name: str, prefix: str) -> Iterator[tuple[object]]:
        ...

    @abstractmethod
    def read_object(self, bucket_name: str, object_name: str) -> bytes:
        ...

    @abstractmethod
    def remove_bucket_if_exists(self, bucket_name: str) -> bool:
        ...
