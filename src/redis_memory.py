import json
import os
from typing import Dict, List

import redis  # type: ignore

from log.logger import get_logger

logger = get_logger(__name__)


class RedisMemory:
    def __init__(
        self,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        window_size: int | None = None,
    ):
        self.host = host or os.environ.get('REDIS_HOST', 'redis')
        self.port = port or int(os.environ.get('REDIS_PORT', 6379))
        self.db = db or int(os.environ.get('REDIS_DB', 0))
        self.window_size = window_size or int(os.environ.get('CHAT_HISTORY_WINDOW_SIZE', 10))
        self._client: redis.Redis | None = None

    def _get_client(self) -> redis.Redis:
        if self._client is None:
            self._client = redis.Redis(
                host=self.host,
                port=self.port,
                db=self.db,
                decode_responses=True,
            )
        return self._client

    @staticmethod
    def _chat_key(chat_id: str) -> str:
        return f'chat:{chat_id}:messages'

    def add_message(self, chat_id: str, role: str, content: str) -> None:
        message = json.dumps({'role': role, 'content': content}, ensure_ascii=False)
        client = self._get_client()
        key = self._chat_key(chat_id)
        pipeline = client.pipeline()
        pipeline.rpush(key, message)
        pipeline.ltrim(key, -self.window_size, -1)
        pipeline.execute()

    def get_last_messages(self, chat_id: str) -> List[Dict[str, str]]:
        client = self._get_client()
        key = self._chat_key(chat_id)
        raw_messages = client.lrange(key, -self.window_size, -1)
        messages: List[Dict[str, str]] = []

        for raw in raw_messages:
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, dict) and 'role' in parsed and 'content' in parsed:
                    messages.append({'role': parsed['role'], 'content': parsed['content']})
            except json.JSONDecodeError:
                logger.warning('Failed to decode redis message for chat_id=%s', chat_id)

        return messages

    def clear_history(self, chat_id: str) -> None:
        self._get_client().delete(self._chat_key(chat_id))

