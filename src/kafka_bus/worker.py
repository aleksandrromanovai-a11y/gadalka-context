"""Entry point for the Kafka consumer/producer worker."""

from __future__ import annotations

import argparse
import json
import os
import signal
from typing import Any, Dict, Optional

from kafka_bus.client import KafkaMessageBus, KafkaSettings, MessageBus
from kafka_bus.mock import InMemoryMessageBus
from log.logger import get_logger
from message_handler import build_message_processor

logger = get_logger(__name__)


def _select_bus(settings: KafkaSettings) -> MessageBus:
    mode = os.environ.get('MESSAGE_BUS_MODE', 'kafka').lower()
    if mode == 'mock':
        logger.info('Using mock message bus (in-memory)')
        return InMemoryMessageBus(settings)
    logger.info('Using real Kafka message bus')
    return KafkaMessageBus(settings)


class KafkaWorker:
    """Long-running worker that consumes messages, processes, and produces output."""

    def __init__(self, bus: MessageBus, settings: KafkaSettings, max_messages: Optional[int] = None):
        self.bus = bus
        self.settings = settings
        self.processor = build_message_processor()
        self.max_messages = max_messages
        self._processed = 0
        self._stopped = False

    def run(self) -> None:
        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_signal)

        logger.info(
            'Kafka worker starting | input=%s output=%s',
            self.settings.input_topic,
            self.settings.output_topic,
        )
        self.bus.start()
        try:
            self.bus.consume(self._handle_message)
        finally:
            self.bus.stop()
            logger.info('Kafka worker stopped')

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        logger.info('Received signal %s, stopping worker', signum)
        self.stop()

    def stop(self) -> None:
        if self._stopped:
            return
        self._stopped = True
        self.bus.stop()

    def _handle_message(self, raw_payload: str, metadata: Dict[str, Any]) -> bool:
        if self._stopped:
            return False

        message_text, payload_meta = self._parse_payload(raw_payload)
        merged_metadata = self._merge_metadata(metadata, payload_meta, raw_payload)

        result_text = self.processor(message_text, merged_metadata) or ''

        output_body = {'response_text': result_text}
        output_headers = self._response_headers(merged_metadata)
        self.bus.produce(self.settings.output_topic, output_body, headers=output_headers)

        self._processed += 1
        if self.max_messages is not None and self._processed >= self.max_messages:
            logger.info('Processed max_messages=%s, stopping', self.max_messages)
            self.stop()

        return True

    def _parse_payload(self, raw_payload: str) -> tuple[str, Dict[str, Any]]:
        try:
            parsed = json.loads(raw_payload)
        except json.JSONDecodeError:
            return raw_payload, {}

        if isinstance(parsed, dict):
            message_text = parsed.get('request_text') or parsed.get('message') or parsed.get('payload') or ''
            metadata = parsed.get('metadata') if isinstance(parsed.get('metadata'), dict) else {}
            if not message_text:
                message_text = json.dumps(parsed, ensure_ascii=False)
            return str(message_text), metadata

        if isinstance(parsed, str):
            return parsed, {}

        return json.dumps(parsed, ensure_ascii=False), {}

    def _merge_metadata(self, metadata: Dict[str, Any], payload_meta: Dict[str, Any], raw_payload: str) -> Dict[str, Any]:
        # Do not mutate original metadata dict coming from the transport
        headers = dict((metadata.get('headers') or {}))

        request_id = headers.get('request_id') or headers.get('request-id')
        bot_id = headers.get('bot_id') or headers.get('bot-id')
        chat_id = headers.get('chat_id') or headers.get('chat-id')
        natal_chart_raw = headers.get('natal_chart') or headers.get('natal-chart')

        natal_chart = natal_chart_raw
        if isinstance(natal_chart_raw, str):
            try:
                natal_chart = json.loads(natal_chart_raw)
            except json.JSONDecodeError:
                natal_chart = natal_chart_raw

        merged = {**metadata, **payload_meta}
        merged.update(
            {
                'request_id': request_id,
                'bot_id': bot_id,
                'chat_id': chat_id,
                'natal_chart': natal_chart,
                # Provide explicit IDs for the LLM memory layer
                'user_id': bot_id or merged.get('user_id'),
                'session_id': chat_id or merged.get('session_id'),
                # Convenience copies to pass full context into get_response
                'input_headers': headers,
                'input_payload': payload_meta,
                'raw_payload': raw_payload,
            }
        )
        return merged

    def _response_headers(self, metadata: Dict[str, Any]) -> Dict[str, Any]:
        headers = {}
        for key in ('request_id', 'bot_id', 'chat_id'):
            value = metadata.get(key)
            if value is not None:
                headers[key] = value
        return headers


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Kafka worker to consume, process, and produce messages.')
    parser.add_argument(
        '--max-messages',
        type=int,
        default=None,
        help='Optional limit for messages to process before stopping (useful for tests).',
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    settings = KafkaSettings()
    bus = _select_bus(settings)
    worker = KafkaWorker(bus, settings, max_messages=args.max_messages)
    worker.run()


if __name__ == '__main__':
    main()

