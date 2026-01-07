import signal
import string
import datetime
import json
from time import time, sleep
from queue import Empty
from typing import List, Any, Dict, Union, Optional

from .environment import CFGKafka
from .producer import KafkaMsgProducer, KafkaProducerInput
from .consumer import KafkaMsgConsumer, KafkaConsumerOutput
from log.logger import get_logger



logger = get_logger(__name__)


class GracefulKiller():
    kill_now = False
    def __init__(self) -> None:
        signal.signal(signal.SIGINT, self._exit_gracefully)
        signal.signal(signal.SIGTERM, self._exit_gracefully)

    def _exit_gracefully(self, *args, **kwargs) -> None:
        self.kill_now = True


class Orchestrator:

    def __init__(self, cfg: CFGKafka):
        self.cfg = cfg
        self.cfg.log_summary()
        self.consumer = KafkaMsgConsumer(cfg=self.cfg)
        self.producer = KafkaMsgProducer(cfg=self.cfg)
        self.killer = GracefulKiller()

        self.batch_size = self.cfg.BATCH_SIZE
        self.timeout = 5.0

        self.queue = self.consumer.get_msg_queue()
        self.consumer.start()
        self.stopped = False
        self._wait_until_consumer_starts()
        return None

    def _wait_until_consumer_starts(self, timeout: float = 10.):
        end_time = time() + timeout
        while time() < end_time:
            if self.consumer.is_started():
                return None
            sleep(0.1)
        raise ConnectionError(f'Could not connect to Kafka {self.consumer.cfg}')

    def __iter__(self):
        return self

    def __next__(self) -> List[KafkaConsumerOutput]:
        while True:
            batch = []

            while len(batch) < self.batch_size:
                if self.killer.kill_now and not self.stopped:
                    self.consumer.stop()
                    self.stopped = True

                try:
                    msg: KafkaConsumerOutput = self.queue.get(timeout=5.)
                except Empty:
                    logger.info('Pending images')
                    if batch:
                        break
                    continue

                if msg is None:
                    self.producer.flush()
                    logger.info('All messages from the inner buffer were processed')
                    raise StopIteration

                batch.append(msg)
            return batch

    def send_result(self,
                    timestamp: int,
                    offset: int,
                    key: str,
                    request_id: str,
                    response_text: str,
                    chat_id: str,
                    bot_id: str,
                    ) -> None:

        '''
        {
            "request_id": "f91fb30a-124b-4de0-b668-5d0b5ea6d6e7",
            "response_text": "гавнаед",
            "chat_id": 363642145,
            "bot_id": "astro1"
        }
        '''
        

        headers = [
            (self.cfg.HEADER_TIMESTAMP_ENCODING, self.time_unix2rfc3339(timestamp / 1000).encode())
        ]
        if offset is not None:
            if isinstance(offset, int):
                offset = str(offset).encode('utf-8')
            headers.append((self.cfg.HEADER_OFFSET_ENCODING, offset))

        response_value = {
            "request_id": request_id,
            "response_text": response_text,
            "chat_id": int(chat_id),
            "bot_id": bot_id
        }

        msg = KafkaProducerInput(value=json.dumps(response_value, ensure_ascii=False).encode('utf-8'), key=str(key).encode('utf-8'), headers=headers)
        self.producer.send(msg)
        return None

    @staticmethod
    def time_unix2rfc3339(timestamp: float):
        return datetime.datetime.fromtimestamp(timestamp).astimezone().isoformat()
