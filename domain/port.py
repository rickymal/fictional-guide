from __future__ import annotations

from abc import ABC, abstractmethod
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import IO, Any, Dict, Iterator, List, Optional, Union


class IBrokerAdapter(ABC):
    @abstractmethod
    def __init__(self, env: dict): ...

    @abstractmethod
    def setup_infrastructure(self, env: dict): ...

    @abstractmethod
    def publish_message(
        self, routing_key: str, message: str, count: int = 0
    ) -> None: ...

    @abstractmethod
    def consume_sync(self, qtd: int) -> list: ...

    @abstractmethod
    def consume_blocking(self, callback_default: object, callback_dlq: object): ...

    @abstractmethod
    def acknowledge_message(self, delivery_tag: int): ...

    @abstractmethod
    def reject_message(
        self, delivery_tag: int, count: int, message: dict, routing_key: str
    ): ...

    @abstractmethod
    def close(self): ...
