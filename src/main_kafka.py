from __future__ import annotations

from typing import Optional

from environment import Config
from kafka_bus.communication import Orchestrator
from kafka_bus.environment import CFGKafka
from log.logger import get_logger
from llm_utils.llm_response import Responser

logger = get_logger(__name__)


def _process_message(responser: Responser, orchestrator: Orchestrator) -> None:
    """Consume messages from Kafka, get LLM response, and publish the result."""
    for batch in orchestrator:
        for msg in batch:
            try:
                response_text: Optional[str] = responser.get_response(msg)
                if response_text is None:
                    response_text = ''

                if response_text or orchestrator.producer.should_send_empty():
                    orchestrator.send_result(
                        timestamp=msg.timestamp,
                        offset=msg.offset,
                        key=msg.key,
                        request_id=msg.request_id,
                        response_text=response_text,
                        chat_id=msg.chat_id,
                        bot_id=msg.bot_id,
                    )
            except Exception as exc:
                logger.error('Failed to process message: %s', exc, exc_info=True)


def main() -> None:
    cfg = Config()
    kafka_cfg = CFGKafka()

    responser = Responser(cfg)
    orchestrator = Orchestrator(cfg=kafka_cfg)

    _process_message(responser, orchestrator)


if __name__ == '__main__':
    main()

