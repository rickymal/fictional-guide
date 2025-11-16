from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import IO, Any, Dict, Generator, Iterator, List, Optional, Union


class IStorageConnectionAdapter(ABC):
    @classmethod
    @abstractmethod
    def from_duckdb_memory(cls, env: dict) -> "IStorageConnectionAdapter": ...

    @abstractmethod
    def __init__(self): ...

    @abstractmethod
    def get_connection(self): ...

    @abstractmethod
    def close_connection(self): ...

    @abstractmethod
    def connect(self): ...

    @abstractmethod
    def create_transaction(self) -> Generator[object]: ...
