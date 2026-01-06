from typing import Dict, Any
from dataclasses import dataclass, field
import os

from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class TEIDeepVkUserBgeM3:
    """
    https://huggingface.co/deepvk/USER-bge-m3
    """
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'embedder': {
                'provider': 'huggingface', 
                'config': {
                    'huggingface_base_url': '',
                    'api_key': 'not-needed',  # Фиктивный ключ для локального TEI сервера
                }
            }
        })
        
    def __post_init__(self):
        self._log_warnings()
        self.config['embedder']['config']['huggingface_base_url'] = os.environ.get('TEI_EMBEDDING_MODEL_URL')
        # Используем api_key из переменной окружения (может быть фиктивным для локального TEI)
        # Убираем пробелы, чтобы избежать ошибки "Illegal header value"
        api_key = os.environ.get('OPENAI_API_KEY', 'not-needed').strip()
        self.config['embedder']['config']['api_key'] = api_key if api_key else 'not-needed'
        logger.info(self.config)
    
    def _log_warnings(self):
        if os.environ.get('TEI_EMBEDDING_MODEL_URL') is None:
            logger.warning('TEI Embedding Model URL not found. Please set the TEI_EMBEDDING_MODEL_URL environment variable.')
            

@dataclass
class Mem0OpenaiEmbeddingModel:
    """
    https://docs.mem0.ai/components/embedders/models/openai
    """
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'embedder': {
                'provider': 'openai', 
                'config': {
                    'model': 'takes from env',
                }
            }
        })
        
    def __post_init__(self):
        self._log_warnings()
        self.config['embedder']['config']['model'] = str(os.environ.get('MEMORY_LLM_EMBEDDINGS_MODEL', 'text-embedding-3-large'))
        logger.info(self.config)
    
    def _log_warnings(self):
        if os.environ.get('MEMORY_LLM_EMBEDDINGS_MODEL') is None:
            logger.warning('MEMORY_LLM_EMBEDDINGS_MODEL not found. Please set the MEMORY_LLM_EMBEDDINGS_MODEL environment variable.')
            
    