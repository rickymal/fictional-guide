from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Optional, List, Dict, Union, IO, Iterator, Generator
from datetime import datetime, timedelta

class IBrokerAdapter(ABC):
    @abstractmethod
    def __init__(self, env: dict):
        ...

    @abstractmethod
    def setup_infrastructure(self, env: dict):
        ...

    @abstractmethod
    def publish_message(self, routing_key: str, message: str, count: int=0) -> None:
        ...

    @abstractmethod
    def consume_sync(self, qtd: int) -> list:
        ...

    @abstractmethod
    def stop_after_duration(self, duration):
        ...

    @abstractmethod
    def consume_blocking(self, callback_default: object, callback_dlq: object, duration: object=None):
        ...

    @abstractmethod
    def acknowledge_message(self, delivery_tag: int):
        ...

    @abstractmethod
    def reject_message(self, delivery_tag: int, count: int, message: str, routing_key: str):
        ...

    @abstractmethod
    def close(self):
        ...
