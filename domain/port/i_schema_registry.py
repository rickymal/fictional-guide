from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, List, Dict, Union, IO, Iterator, Generator
from datetime import datetime, timedelta

class ISchemaRegistry(ABC):
    @abstractmethod
    def __init__(self):
        ...

    @abstractmethod
    def get_avro_schema_by_namespace(self, conn: object, namespace: str):
        ...

    @abstractmethod
    def initialize_schema(self, conn: object):
        ...

    @abstractmethod
    def insert_schema(self, conn: object, schema: str) -> str:
        ...

    @abstractmethod
    def delete_schema(self, conn: object, namespace: object=None) -> str:
        ...

    @abstractmethod
    def get_all(self, conn: object):
        ...
