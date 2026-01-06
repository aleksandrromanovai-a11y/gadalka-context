from typing import Dict, Any
from dataclasses import dataclass, field
import os

from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class RerankerDeepSeekLLMProvider:
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'reranker':{
                'provider': 'llm_reranker',
                'config': {
                    'llm': {
                        'provider': 'deepseek', 
                        'config': {
                            'model': 'deepseek-chat',
                            'api_key': os.environ.get('RERANKER_API_KEY', ''),
                            'temperature': 0.0,
                            'base_url': 'https://api.deepseek.com',
                            
                        }
                    }
                }
            }
        })
        
    def __post_init__(self):
        self._log_warnings()
    
    def _log_warnings(self):
        if os.environ.get('RERANKER_API_KEY') is None:
            logger.warning('Reranker API key not found. Please set the RERANKER_API_KEY environment variable.')
            

@dataclass
class RerankerOpenaiLLMProvider:
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'reranker':{
                'provider': 'llm_reranker',
                'config': {
                    'llm': {
                        'provider': 'openai', 
                        'config': {
                            'model': 'takes from env',
                            'temperature': 0.0,
                            'api_key': 'takes from env',
                            
                        }
                    }
                }
            }
        })
        
    def __post_init__(self):
        self._log_warnings()
        self.config['reranker']['config']['llm']['config']['model'] = str(os.environ.get('MEMORY_LLM_RERANKER_MODEL', 'gpt-4.1-nano-2025-04-14'))
        self.config['reranker']['config']['llm']['config']['api_key'] = str(os.environ.get('OPENAI_API_KEY', ''))
        self.config['reranker']['config']['llm']['provider'] = str(os.environ.get('MEMORY_RERANKER_PROVIDER', 'openai'))
    
    def _log_warnings(self):
        if os.environ.get('MEMORY_LLM_RERANKER_MODEL') is None:
            logger.warning('Reranker API key not found. Please set the RERANKER_API_KEY environment variable.')
        if os.environ.get('OPENAI_API_KEY') is None:
            logger.warning('OPENAI_API_KEY key not found. Please set the OPENAI_API_KEY environment variable.')
        if os.environ.get('MEMORY_RERANKER_PROVIDER') is None:
            logger.warning('MEMORY_RERANKER_PROVIDER not found. Please set the MEMORY_RERANKER_PROVIDER environment variable.')