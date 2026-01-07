"""Self-contained mock backend runner: feed request, get response from in-memory topics."""

from __future__ import annotations

import argparse
import json
from typing import Any, Dict

from kafka_bus.client import KafkaSettings
from kafka_bus.mock import InMemoryMessageBus
from kafka_bus.worker import KafkaWorker


def _parse_natal_chart(raw: str | None) -> Any:
    if raw is None:
        return None
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        return raw


def build_headers(args: argparse.Namespace) -> Dict[str, Any]:
    headers: Dict[str, Any] = {}
    if args.request_id:
        headers['request_id'] = args.request_id
    if args.bot_id:
        headers['bot_id'] = args.bot_id
    if args.chat_id:
        headers['chat_id'] = args.chat_id
    if args.natal_chart:
        headers['natal_chart'] = _parse_natal_chart(args.natal_chart)
    return headers


def run_once(args: argparse.Namespace) -> None:
    settings = KafkaSettings()
    bus = InMemoryMessageBus(settings)
    worker = KafkaWorker(bus, settings, max_messages=1)

    headers = build_headers(args)
    payload: Dict[str, Any] = {'request_text': args.request_text}

    bus.enqueue_input(payload, metadata={'headers': headers})
    worker.run()

    outputs = bus.get_topic_messages(settings.output_topic)
    if not outputs:
        print('No output messages produced.')
        return

    print(json.dumps(outputs, ensure_ascii=False, indent=2))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Run mock backend end-to-end in memory.')
    parser.add_argument('--request-text', required=True, help='Text to send as request_text')
    parser.add_argument('--request-id', help='request_id header')
    parser.add_argument('--bot-id', help='bot_id header (used as user_id)')
    parser.add_argument('--chat-id', help='chat_id header (used as session_id)')
    parser.add_argument('--natal-chart', help='natal_chart header (JSON string or plain text)')
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    run_once(args)


if __name__ == '__main__':
    main()

