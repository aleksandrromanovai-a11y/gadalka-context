from dataclasses import dataclass
from typing import List, Optional, Union
import os

from log.logger import get_logger

logger = get_logger(__name__)

@dataclass
class CFGKafka:
    # consumer
    BOOTSTRAP_SERVERS: List[str] = None
    INPUT_TOPIC: str = None
    GROUP_ID: str = None  # KAFKA_CONSUMER_GROUP
    ENABLE_AUTO_COMMIT: bool = True
    HEARTBEAT_INTERVAL_MS: int = 10_000
    SESSION_TIMEOUT_MS: int = 60_000
    START_OFFSET: Optional[int] = None
    END_OFFSET: Optional[int] = None
    CONSUMER_TIMEOUT_MS: Union[int, float] = 5_000  # float('inf')
    MAX_PARTITION_FETCH_BYTES: int = 1_000_000
    FETCH_MAX_BYTES: int = 52_428_800

    # producer
    OUTPUT_TOPIC: str = None
    KAFKA_BATCH_SIZE: int = 16384
    LINGER_MS: int = 0
    SHOULD_SEND_EMPTY = True

    # headers
    HEADER_TIMESTAMP_ENCODING: str = 'image_timestamp'
    HEADER_IMAGE_ID_ENCODING: str = 'image_id'
    HEADER_OFFSET_ENCODING: str = 'src_offset'

    # image download
    IMAGE_DOWNLOAD_TIMEOUT: float = 3.0
    IMAGE_DOWNLOAD_VERIFY: bool = False

    LABELS_LIST: List[str] = None
    TASK_TYPE: str = None   # one of 'classification', 'detection', 'segmentation', 'change_detection' for correct json format
    BATCH_SIZE: int = 1

    def __post_init__(self):
        if 'KAFKA_BOOTSTRAP_SERVERS' in os.environ:
            self.BOOTSTRAP_SERVERS = os.environ['KAFKA_BOOTSTRAP_SERVERS'].split(',')
        if 'KAFKA_INPUT_TOPIC' in os.environ:
            self.INPUT_TOPIC = os.environ['KAFKA_INPUT_TOPIC']
        if 'KAFKA_OUTPUT_TOPIC' in os.environ:
            self.OUTPUT_TOPIC = os.environ['KAFKA_OUTPUT_TOPIC']
        if 'ENABLE_AUTO_COMMIT' in os.environ:
            self.ENABLE_AUTO_COMMIT = bool(int(os.environ['ENABLE_AUTO_COMMIT']))
        if 'KAFKA_CONSUMER_GROUP' in os.environ:
            self.GROUP_ID = os.environ['KAFKA_CONSUMER_GROUP']
        if 'KAFKA_HEARTBEAT_INTERVAL_MS' in os.environ:
            self.HEARTBEAT_INTERVAL_MS = int(os.environ['KAFKA_HEARTBEAT_INTERVAL_MS'])
        if 'KAFKA_SESSION_TIMEOUT_MS' in os.environ:
            self.SESSION_TIMEOUT_MS = int(os.environ['KAFKA_SESSION_TIMEOUT_MS'])
        if 'KAFKA_START_OFFSET' in os.environ:
            self.START_OFFSET = int(os.environ['KAFKA_START_OFFSET'])
        if 'KAFKA_END_OFFSET' in os.environ:
            self.END_OFFSET = int(os.environ["KAFKA_END_OFFSET"])
        if 'KAFKA_CONSUMER_TIMEOUT_MS' in os.environ:
            self.CONSUMER_TIMEOUT_MS = int(os.environ['KAFKA_CONSUMER_TIMEOUT_MS'])
        if 'KAFKA_BATCH_SIZE' in os.environ:
            self.KAFKA_BATCH_SIZE = int(os.environ['KAFKA_BATCH_SIZE'])
        if 'KAFKA_LINGER_MS' in os.environ:
            self.LINGER_MS = int(os.environ['KAFKA_LINGER_MS'])
        if 'KAFKA_SHOULD_SEND_EMPTY_RESULT' in os.environ:
            self.SHOULD_SEND_EMPTY = bool(os.environ['KAFKA_SHOULD_SEND_EMPTY_RESULT'])
        if 'MAX_PARTITION_FETCH_BYTES' in os.environ:
            self.MAX_PARTITION_FETCH_BYTES = int(os.environ['MAX_PARTITION_FETCH_BYTES'])
        if 'FETCH_MAX_BYTES' in os.environ:
            self.FETCH_MAX_BYTES = int(os.environ['FETCH_MAX_BYTES'])
        if 'IMAGE_DOWNLOAD_VERIFY' in os.environ:
            self.IMAGE_DOWNLOAD_VERIFY = bool(int(os.environ['IMAGE_DOWNLOAD_VERIFY']))
        return None
    
    def log_summary(self):
        separator = '═' * 80
        def log_section(title: str, obj: object):
            logger.info(separator)
            logger.info(f' {title.upper()} CONFIG')
            logger.info(separator)
            for k, v in vars(obj).items():
                if k == 'model':
                    continue
                logger.info(f'{k:<25}: {v}')
            logger.info('')

        log_section('KAFKA-GIGA-ORCH CFG', self)
        logger.info(separator)
