from typing import Dict, Any
from dataclasses import dataclass, field
import os

from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class QdrantStore:
    """
    http://localhost:6333/dashboard
    """
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'provider': 'qdrant', 
            'config': {
                'host': 'localhost',
                'port': 6333,
                'collection_name': 'test-gadalka',
                'embedding_model_dims': 1536  # Размерность векторов для bge-m3
            }
        })
    
    def __post_init__(self):
        host = os.environ.get('QDRANT_HOST', 'localhost')
        port = int(str(os.environ.get('QDRANT_PORT', 6333)))
        collection = os.environ.get('QDRANT_COLLECTION', 'test-gadalka')
        dims = int(str(os.environ.get('QDRANT_EMBEDDING_DIMS', 1536)))
        
        self.config['config']['host'] = host
        self.config['config']['port'] = port
        self.config['config']['collection_name'] = collection
        self.config['config']['embedding_model_dims'] = dims
        logger.info(self.config)
    