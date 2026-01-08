import json
import os
from typing import Dict, List

import redis  # type: ignore

from log.logger import get_logger

logger = get_logger(__name__)


class RedisMemory:
    def __init__(
        self,
        url: str | None = None,
        host: str | None = None,
        port: int | None = None,
        db: int | None = None,
        username: str | None = None,
        password: str | None = None,
        window_size: int | None = None,
    ):
        self.url = url or os.environ.get('REDIS_URL') or os.environ.get('REDIS_PUBLIC_URL')
        self.host = host or os.environ.get('REDIS_HOST') or os.environ.get('REDISHOST') or 'redis'
        self.port = port if port is not None else int(
            os.environ.get('REDIS_PORT') or os.environ.get('REDISPORT') or 6379
        )
        self.db = db if db is not None else int(os.environ.get('REDIS_DB') or 0)
        self.username = username or os.environ.get('REDIS_USERNAME') or os.environ.get('REDISUSER')
        self.password = password or os.environ.get('REDIS_PASSWORD') or os.environ.get('REDISPASSWORD')
        self.window_size = (
            window_size if window_size is not None else int(os.environ.get('CHAT_HISTORY_WINDOW_SIZE') or 10)
        )
        self._client: redis.Redis | None = None

    def _get_client(self) -> redis.Redis:
        if self._client is None:
            if self.url:
                self._client = redis.from_url(
                    self.url,
                    db=self.db,
                    decode_responses=True,
                )
            else:
                self._client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    username=self.username,
                    password=self.password,
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

