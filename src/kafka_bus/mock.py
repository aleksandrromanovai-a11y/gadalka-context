"""In-memory mock of the Kafka message bus for local testing."""

from __future__ import annotations

import json
import threading
from collections import defaultdict
from typing import Any, Dict, List, Tuple

from kafka_bus.client import KafkaSettings, MessageBus, MessageHandler
from log.logger import get_logger

logger = get_logger(__name__)


class InMemoryMessageBus(MessageBus):
    """Simple in-memory transport to simulate Kafka topics."""

    def __init__(self, settings: KafkaSettings):
        self.settings = settings
        self._running = threading.Event()
        self._input_queue: List[Tuple[str, Dict[str, Any]]] = []
        self._topics: dict[str, list[Dict[str, Any]]] = defaultdict(list)

    def start(self) -> None:
        self._running.set()

    def stop(self) -> None:
        self._running.clear()

    def produce(self, topic: str, message: Dict[str, Any] | str, headers: Dict[str, Any] | None = None) -> None:
        payload = message if isinstance(message, str) else json.dumps(message, ensure_ascii=False)
        entry = {'value': payload, 'headers': headers or {}}
        self._topics[topic].append(entry)
        logger.debug('Mock produce to %s: %s | headers=%s', topic, payload, headers)

    def consume(self, handler: MessageHandler, max_messages: int | None = None) -> None:
        if not self._running.is_set():
            self.start()

        processed = 0

        while self._running.is_set() and self._input_queue:
            payload, metadata = self._input_queue.pop(0)
            try:
                success = handler(payload, metadata)
            except Exception as exc:
                success = self._handle_failure(payload, metadata, exc)

            if success:
                processed += 1
            else:
                # Requeue for another attempt when the handler fails
                self._input_queue.append((payload, metadata))

            if max_messages is not None and processed >= max_messages:
                break

        if not self._input_queue:
            logger.debug('Mock queue drained')

    def enqueue_input(self, message: Dict[str, Any] | str, metadata: Dict[str, Any] | None = None) -> None:
        payload = message if isinstance(message, str) else json.dumps(message, ensure_ascii=False)
        self._input_queue.append((payload, metadata or {}))

    def get_topic_messages(self, topic: str) -> list[Dict[str, Any]]:
        return list(self._topics.get(topic, []))

    def _handle_failure(self, payload: str, metadata: Dict[str, Any], error: Exception) -> bool:
        logger.error('Mock handler failed: %s | payload=%s', error, payload, exc_info=True)
        logger.debug('Message will be requeued for retry')
        return False

