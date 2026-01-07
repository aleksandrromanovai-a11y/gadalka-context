"""Simple mock client to interact with the FastAPI/uvicorn server."""

import argparse
import json
import os
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional

# Добавляем путь к src в PYTHONPATH для импорта модулей
src_path = Path(__file__).parent.parent
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))

from kafka_bus.consumer import KafkaConsumerOutput


def _request(
    method: str,
    url: str,
    data: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
) -> str:
    req = urllib.request.Request(url, data=data, headers=headers or {}, method=method)
    try:
        with urllib.request.urlopen(req) as resp:
            body = resp.read()
            return body.decode('utf-8', errors='replace')
    except urllib.error.HTTPError as exc:  # pragma: no cover - CLI helper
        return f'HTTP {exc.code}: {exc.reason}\n{exc.read().decode("utf-8", errors="replace")}'
    except urllib.error.URLError as exc:  # pragma: no cover - CLI helper
        return f'Connection error: {exc.reason}'


def send_text(base_url: str, text: str) -> str:
    return _request(
        'POST',
        base_url,
        data=text.encode('utf-8'),
        headers={'Content-Type': 'text/plain; charset=utf-8'},
    )


def send_json(base_url: str, payload: Any) -> str:
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    return _request(
        'POST',
        base_url,
        data=data,
        headers={'Content-Type': 'application/json'},
    )


def get_health(base_url: str) -> str:
    return _request('GET', base_url)


def create_kafka_consumer_output(
    request_text: str,
    request_id: Optional[str] = None,
    bot_id: Optional[str] = None,
    chat_id: Optional[str] = None,
    natal_chart: Optional[str] = None,
    timestamp: Optional[int] = None,
    key: Optional[str] = None,
    offset: Optional[int] = None,
) -> KafkaConsumerOutput:
    """Создает KafkaConsumerOutput из переданных данных."""
    return KafkaConsumerOutput(
        timestamp=timestamp or int(time.time() * 1000),
        key=key or '',
        offset=offset or 0,
        request_text=request_text,
        request_id=request_id or 'mock_request_id',
        bot_id=bot_id or 'mock_bot_id',
        chat_id=chat_id or 'mock_chat_id',
        natal_chart=natal_chart or '',
    )


def send_kafka_format(
    base_url: str,
    request_text: str,
    request_id: Optional[str] = None,
    bot_id: Optional[str] = None,
    chat_id: Optional[str] = None,
    natal_chart: Optional[str] = None,
) -> str:
    """Отправляет сообщение в формате Kafka (payload с request_text и headers)."""
    payload = {'request_text': request_text}
    headers = {
        'Content-Type': 'application/json',
        'request_id': request_id or 'mock_request_id',
        'bot_id': bot_id or 'mock_bot_id',
        'chat_id': chat_id or 'mock_chat_id',
    }
    if natal_chart:
        headers['natal_chart'] = natal_chart
    
    data = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    return _request('POST', base_url, data=data, headers=headers)


def main() -> None:
    parser = argparse.ArgumentParser(description='Mock client for the echo server.')
    parser.add_argument('--host', default='127.0.0.1', help='Server host.')
    parser.add_argument('--port', type=int, default=8000, help='Server port.')
    parser.add_argument(
        '--mode',
        choices=['text', 'json', 'health', 'kafka'],
        default='text',
        help='Request type to send.',
    )
    parser.add_argument(
        '--data',
        help='Text payload or JSON string (for --mode json).',
    )
    parser.add_argument('--request-id', help='Request ID for kafka mode.')
    parser.add_argument('--bot-id', help='Bot ID for kafka mode.')
    parser.add_argument('--chat-id', help='Chat ID for kafka mode.')
    parser.add_argument('--natal-chart', help='Natal chart for kafka mode.')
    parser.add_argument('--show-output', action='store_true', help='Show created KafkaConsumerOutput object.')
    args = parser.parse_args()

    base_url = f'http://{args.host}:{args.port}/'

    if args.mode == 'health':
        print(get_health(base_url))
        return

    if args.mode == 'kafka':
        request_text = args.data or 'hello'
        kafka_output = create_kafka_consumer_output(
            request_text=request_text,
            request_id=args.request_id,
            bot_id=args.bot_id,
            chat_id=args.chat_id,
            natal_chart=args.natal_chart,
        )
        if args.show_output:
            print('Created KafkaConsumerOutput:', file=sys.stderr)
            print(f'  timestamp: {kafka_output.timestamp}', file=sys.stderr)
            print(f'  key: {kafka_output.key}', file=sys.stderr)
            print(f'  offset: {kafka_output.offset}', file=sys.stderr)
            print(f'  request_text: {kafka_output.request_text}', file=sys.stderr)
            print(f'  request_id: {kafka_output.request_id}', file=sys.stderr)
            print(f'  bot_id: {kafka_output.bot_id}', file=sys.stderr)
            print(f'  chat_id: {kafka_output.chat_id}', file=sys.stderr)
            print(f'  natal_chart: {kafka_output.natal_chart}', file=sys.stderr)
        print(send_kafka_format(
            base_url,
            request_text,
            request_id=args.request_id,
            bot_id=args.bot_id,
            chat_id=args.chat_id,
            natal_chart=args.natal_chart,
        ))
        return

    if args.mode == 'text':
        payload = args.data or 'hello'
        print(send_text(base_url, payload))
        return

    # json mode
    if args.data:
        try:
            payload_obj = json.loads(args.data)
        except json.JSONDecodeError:
            print('Invalid JSON in --data', file=sys.stderr)
            sys.exit(1)
    else:
        payload_obj = {'msg': 'hello'}

    print(send_json(base_url, payload_obj))


if __name__ == '__main__':
    main()
