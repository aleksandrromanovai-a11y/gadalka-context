"""Thin adapter to reuse existing business logic in different transports."""

from __future__ import annotations

from typing import Any, Callable, Dict

from environment import Config
from llm_utils.llm_response import Responser
from log.logger import get_logger

logger = get_logger(__name__)

MessageProcessor = Callable[[str, Dict[str, Any]], str | None]


def build_message_processor() -> MessageProcessor:
    """Construct the existing business logic handler."""
    config = Config()
    responser = Responser(config)
    logger.info('Message processor initialized with Responser')
    return responser.get_response

