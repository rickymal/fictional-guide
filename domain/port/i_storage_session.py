from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime, timedelta
from typing import IO, Any, Dict, Generator, Iterator, List, Optional, Union


class IStorageSession(ABC):
    @abstractmethod
    def execute(self, query: str, parameters: object = None) -> "IStorageSession": ...

    @abstractmethod
    def fetchall(self) -> list[tuple[object, ...]]: ...

    @abstractmethod
    def begin(self) -> "IStorageSession": ...

    @abstractmethod
    def commit(self) -> "IStorageSession": ...

    @abstractmethod
    def rollback(self) -> "IStorageSession": ...

    @abstractmethod
    def close(self) -> None: ...
