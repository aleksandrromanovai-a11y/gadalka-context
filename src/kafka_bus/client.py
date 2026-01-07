"""Kafka transport implementation with a small message-bus abstraction."""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from typing import Any, Callable, Dict, Optional

try:
    from confluent_kafka import Consumer, Producer  # type: ignore
except ImportError as confluent_import_error:  # pragma: no cover - import guard
    Consumer = None
    Producer = None
    _confluent_import_error = confluent_import_error
else:
    _confluent_import_error = None

from log.logger import get_logger

MessageHandler = Callable[[str, Dict[str, Any]], bool]

logger = get_logger(__name__)


def _strtobool(value: str | None, default: bool = False) -> bool:
    if value is None:
        return default
    return value.lower() in {'1', 'true', 'yes', 'y', 'on'}


@dataclass
class KafkaSettings:
    """Environment-driven settings for Kafka connectivity and topics."""

    bootstrap_servers: str = os.environ.get('KAFKA_BOOTSTRAP_SERVERS', 'redpanda:9092')
    group_id: str = os.environ.get('KAFKA_GROUP_ID', 'gadalka-consumer')
    input_topic: str = os.environ.get('KAFKA_INPUT_TOPIC', 'gadalka-input')
    output_topic: str = os.environ.get('KAFKA_OUTPUT_TOPIC', 'gadalka-output')
    security_protocol: Optional[str] = os.environ.get('KAFKA_SECURITY_PROTOCOL') or None
    sasl_mechanism: Optional[str] = os.environ.get('KAFKA_SASL_MECHANISM') or None
    sasl_username: Optional[str] = os.environ.get('KAFKA_USERNAME') or None
    sasl_password: Optional[str] = os.environ.get('KAFKA_PASSWORD') or None
    auto_offset_reset: str = os.environ.get('KAFKA_AUTO_OFFSET_RESET', 'earliest')

    def consumer_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.group_id,
            'enable.auto.commit': False,
            'auto.offset.reset': self.auto_offset_reset,
        }
        if self.sasl_username and self.sasl_password:
            config.update(
                {
                    'security.protocol': self.security_protocol or 'SASL_SSL',
                    'sasl.mechanisms': self.sasl_mechanism or 'PLAIN',
                    'sasl.username': self.sasl_username,
                    'sasl.password': self.sasl_password,
                }
            )
        elif self.security_protocol:
            config.update({'security.protocol': self.security_protocol})
        return config

    def producer_config(self) -> Dict[str, Any]:
        config: Dict[str, Any] = {'bootstrap.servers': self.bootstrap_servers}
        if self.sasl_username and self.sasl_password:
            config.update(
                {
                    'security.protocol': self.security_protocol or 'SASL_SSL',
                    'sasl.mechanisms': self.sasl_mechanism or 'PLAIN',
                    'sasl.username': self.sasl_username,
                    'sasl.password': self.sasl_password,
                }
            )
        elif self.security_protocol:
            config.update({'security.protocol': self.security_protocol})
        return config


class MessageBus:
    """Minimal interface for pluggable message transports."""

    def start(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def stop(self) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def produce(
        self,
        topic: str,
        message: Dict[str, Any] | str,
        headers: Optional[Dict[str, Any]] = None,
    ) -> None:  # pragma: no cover - interface
        raise NotImplementedError

    def consume(self, handler: MessageHandler) -> None:  # pragma: no cover - interface
        raise NotImplementedError


class KafkaMessageBus(MessageBus):
    """Kafka-backed implementation using confluent-kafka with manual commits."""

    def __init__(self, settings: KafkaSettings, poll_timeout: float = 1.0):
        self.settings = settings
        self.poll_timeout = poll_timeout
        self.consumer: Optional[Consumer] = None
        self.producer: Optional[Producer] = None
        self._running = threading.Event()

    def start(self) -> None:
        if self._running.is_set():
            return
        if Consumer is None or Producer is None:
            raise ImportError(
                'confluent-kafka is required for KafkaMessageBus. '
                'Install it or switch MESSAGE_BUS_MODE=mock.'
            ) from _confluent_import_error
        self.consumer = Consumer(self.settings.consumer_config())
        self.producer = Producer(self.settings.producer_config())
        self.consumer.subscribe([self.settings.input_topic])
        self._running.set()
        logger.info(
            'Kafka consumer subscribed: topic=%s group=%s bootstrap=%s',
            self.settings.input_topic,
            self.settings.group_id,
            self.settings.bootstrap_servers,
        )

    def stop(self) -> None:
        if not self._running.is_set():
            return
        self._running.clear()
        if self.consumer:
            try:
                self.consumer.close()
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning('Kafka consumer close failed: %s', exc, exc_info=True)
        if self.producer:
            try:
                self.producer.flush(5.0)
            except Exception as exc:  # pragma: no cover - defensive
                logger.warning('Kafka producer flush failed: %s', exc, exc_info=True)
        logger.info('Kafka message bus stopped')

    def produce(self, topic: str, message: Dict[str, Any] | str, headers: Optional[Dict[str, Any]] = None) -> None:
        if self.producer is None:
            self.start()
        assert self.producer is not None  # for mypy/static typing
        payload = message if isinstance(message, str) else json.dumps(message, ensure_ascii=False)
        header_list = None
        if headers:
            header_list = []
            for key, value in headers.items():
                if value is None:
                    continue
                header_list.append((str(key), value if isinstance(value, (bytes, bytearray)) else str(value)))
        try:
            self.producer.produce(
                topic,
                value=payload.encode('utf-8'),
                headers=header_list,
                callback=self._delivery_report,
            )
            self.producer.poll(0)
        except BufferError as exc:
            logger.error('Kafka producer queue is full: %s', exc, exc_info=True)
            raise

    def consume(self, handler: MessageHandler) -> None:
        if not self._running.is_set():
            self.start()
        assert self.consumer is not None

        while self._running.is_set():
            msg = self.consumer.poll(self.poll_timeout)
            if msg is None:
                continue
            if msg.error():
                logger.error('Kafka consumer error: %s', msg.error())
                continue

            raw_value = msg.value()
            if raw_value is None:
                logger.warning('Kafka message without value at offset %s', msg.offset())
                continue

            payload = raw_value.decode('utf-8', errors='replace')
            metadata = self._build_metadata(msg)

            try:
                success = handler(payload, metadata)
            except Exception as exc:
                self._handle_failure(payload, metadata, exc, msg)
                continue

            if success:
                try:
                    self.consumer.commit(msg)
                except Exception as exc:  # pragma: no cover - defensive
                    logger.error('Kafka commit failed: %s', exc, exc_info=True)
            else:
                logger.warning('Handler reported failure, message will be reprocessed: %s', metadata)

        self.stop()

    def _delivery_report(self, err: Any, msg: Any) -> None:
        if err:
            logger.error('Kafka delivery failed: %s', err)
        else:
            logger.debug('Kafka message delivered to %s [%s] offset %s', msg.topic(), msg.partition(), msg.offset())

    def _handle_failure(self, payload: str, metadata: Dict[str, Any], error: Exception, msg: Any) -> None:
        logger.error('Message handling failed: %s | payload=%s', error, payload, exc_info=True)

    def _build_metadata(self, msg: Any) -> Dict[str, Any]:
        headers_list = msg.headers() or []
        headers: Dict[str, Any] = {}
        for key, value in headers_list:
            headers[key] = value.decode('utf-8', errors='replace') if isinstance(value, (bytes, bytearray)) else value
        return {
            'topic': msg.topic(),
            'partition': msg.partition(),
            'offset': msg.offset(),
            'key': msg.key(),
            'headers': headers,
        }

