"""Main entrypoint with message processing logic for the FastAPI/uvicorn server."""

import json
import time
from typing import Any, Dict

from kafka_bus.consumer import KafkaConsumerOutput
from llm_utils.llm_response import Responser
from environment import Config
from app import create_app, run_server
from log.logger import get_logger

logger = get_logger(__name__)


def process_message(message: str, metadata: Dict[str, Any]) -> str:
    """Basic processor to adjust later."""
    logger.info(f'Received message: {message}, metadata: {metadata}')
    if not message:
        return 'No payload received.'
    return f'Echo: {message}'


def create_kafka_output_from_http(message: str, metadata: Dict[str, Any]) -> KafkaConsumerOutput:
    """Преобразует HTTP запрос в KafkaConsumerOutput."""
    headers = metadata.get('headers', {})
    
    # Парсим message - может быть JSON с request_text или просто текст
    request_text = message
    try:
        payload = json.loads(message)
        if isinstance(payload, dict) and 'request_text' in payload:
            request_text = payload['request_text']
    except (json.JSONDecodeError, TypeError):
        pass  # Используем message как есть
    
    # Извлекаем данные из headers
    request_id = headers.get('request_id', headers.get('request-id', 'mock_request_id'))
    bot_id = headers.get('bot_id', headers.get('bot-id', 'mock_bot_id'))
    chat_id = headers.get('chat_id', headers.get('chat-id', 'mock_chat_id'))
    natal_chart = headers.get('natal_chart', headers.get('natal-chart', ''))
    
    return KafkaConsumerOutput(
        timestamp=int(time.time() * 1000),
        key='',
        offset=0,
        request_text=request_text,
        request_id=request_id,
        bot_id=bot_id,
        chat_id=chat_id,
        natal_chart=natal_chart,
    )


# Expose app for `uvicorn main:app --reload`
app = create_app(process_message)


def main() -> None:
    import argparse

    parser = argparse.ArgumentParser(
        description='Run the simple text/JSON echo server (uvicorn).'
    )
    parser.add_argument('--host', default='0.0.0.0', help='Host interface to bind.')
    parser.add_argument('--port', type=int, default=8000, help='Port to listen on.')
    args = parser.parse_args()
    
    config = Config()
    
    responser = Responser(config)

    def message_processor(message: str, metadata: Dict[str, Any]) -> str:
        """Адаптер для преобразования HTTP запроса в KafkaConsumerOutput."""
        kafka_output = create_kafka_output_from_http(message, metadata)
        result = responser.get_response(kafka_output)
        return result or ''

    run_server(host=args.host, port=args.port, message_processor=message_processor)


if __name__ == "__main__":
    main()
