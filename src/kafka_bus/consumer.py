import multiprocessing
import time
import json
from dataclasses import dataclass
# from multiprocessing.connection import PipeConnection
from typing import Callable, NoReturn, Optional, Tuple, Any, List, Literal, Union

import httpx
from kafka import KafkaConsumer
from kafka.consumer.fetcher import ConsumerRecord
from kafka.structs import TopicPartition

from .environment import CFGKafka
from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class KafkaConsumerOutput:
    timestamp: int
    key: str  
    offset: int 

    request_text: str
    request_id: str
    bot_id: str
    chat_id: str
    natal_chart: str


class KafkaMsgConsumer(multiprocessing.Process):

    def __init__(self, cfg: CFGKafka, process_msg: Optional[Callable[[ConsumerRecord], KafkaConsumerOutput]] = None):
        super().__init__()
        self.cfg = cfg
        self.process_msg = process_msg if process_msg is not None else self.process_msg

        self.stop_event = multiprocessing.Event()
        self.queue = multiprocessing.Queue(maxsize=100)  # maxsize number of items
        self.started = multiprocessing.Event()
        return None

    def stop(self) -> None:
        """
        The kafka process will not be fully terminated util the self.queue is not empty.
        If to call join() from the main process, it will hang indefinitely in case self.queue is not empty
        After draining the self.queue the kafka process will stop
        """
        logger.info('Shutting down a kafka consumer')
        self.stop_event.set()
        return None

    def get_msg_queue(self):
        return self.queue

    def is_started(self):
        return self.started.is_set()

    def run(self) -> NoReturn:
        logger.info('Starting Kafka consumer process')
        logger.info('Connecting to Kafka for topic pattern %s', self.cfg.INPUT_TOPIC)
        try:
            consumer_kwargs = {
                'bootstrap_servers': self.cfg.BOOTSTRAP_SERVERS,
                'group_id': self.cfg.GROUP_ID,
                'auto_offset_reset': 'earliest',
                'enable_auto_commit': self.cfg.ENABLE_AUTO_COMMIT,
                'heartbeat_interval_ms': self.cfg.HEARTBEAT_INTERVAL_MS,
                'session_timeout_ms': self.cfg.SESSION_TIMEOUT_MS,
                'consumer_timeout_ms': self.cfg.CONSUMER_TIMEOUT_MS,
                'metadata_max_age_ms': 20,
                'max_partition_fetch_bytes': self.cfg.MAX_PARTITION_FETCH_BYTES,
                'fetch_max_bytes': self.cfg.FETCH_MAX_BYTES
            }
            consumer_kwargs.update(self.cfg.auth_kwargs())

            consumer = KafkaConsumer(**consumer_kwargs)
            consumer.subscribe(pattern=self.cfg.INPUT_TOPIC)

            self.started.set()
            logger.info('Connected to Kafka')
            logger.info(f'KAFKA_BOOTSTRAP_SERVER: {self.cfg.BOOTSTRAP_SERVERS} KAFKA_INPUT_TOPIC: {self.cfg.INPUT_TOPIC}')
            while not self.stop_event.is_set():
                for msg in consumer:
                    if self.stop_event.is_set():
                        break
                    if not self.cfg.ENABLE_AUTO_COMMIT:
                        try:
                            consumer.commit()
                        except Exception as e:
                            logger.warning(f'Could not commit a message: {str(e)}', exc_info=True)
                            time.sleep(1.)
                            continue
                        
                    logger.debug(f"Received message {msg.timestamp}")
                    res = self.process_msg(msg)
                    self.queue.put(res)
                # ! think about offsets to stop processing
                else:
                    if self.cfg.END_OFFSET is not None:
                        break
            consumer.close()
        except Exception as e:
            logger.error(e, exc_info=True)
            consumer.close()
        finally:
            self.queue.put(None)
            self.queue.close()
            logger.info('Stopped receiving massages from Kafka')

    def process_msg(self, msg: ConsumerRecord) -> KafkaConsumerOutput:
        '''
        payload: 
        json:"request_text"
        
        headers:
        json:"request_id"
        json:"bot_id" // ID бота для роутинга ответа
        json: "chat_id"
        json:"natal_chart"
        '''
        payload = json.loads(msg.value)
        if not isinstance(payload, dict):
            raise TypeError('Kafka payload must be a JSON object with a request_text field.')

        request_text = payload.get('request_text')
        natal_chart = payload.get('natal_chart')
        if natal_chart is None:
            natal_chart = ''
            logger.warning('natal_chart is not provided in kafka payload.')
        if not isinstance(natal_chart, str):
            natal_chart = str(natal_chart)
        if request_text is None:
            raise KeyError('request_text is required in kafka payload.')
        if not isinstance(request_text, str):
            request_text = str(request_text)

        headers: dict[str, Any] = {}
        for raw_header in msg.headers or []:
            key, value = raw_header
            header_key = key.decode('utf-8', errors='replace') if isinstance(key, bytes) else str(key)

            if isinstance(value, bytes):
                header_value = value.decode('utf-8', errors='replace')
            elif isinstance(value, (dict, list)):
                header_value = json.dumps(value, ensure_ascii=False)
            elif value is None:
                header_value = ''
            else:
                header_value = str(value)

            headers[header_key] = header_value

        return KafkaConsumerOutput(
            timestamp=msg.timestamp,
            key=msg.key.decode('utf-8', errors='replace') if isinstance(msg.key, bytes) else str(msg.key),
            offset=msg.offset,
            request_text=request_text,
            request_id=headers['request_id'],
            bot_id=headers['bot_id'],
            chat_id=headers['chat_id'],
            natal_chart=natal_chart,
        )