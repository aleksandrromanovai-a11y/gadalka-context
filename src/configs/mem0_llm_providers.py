from typing import Dict, Any
from dataclasses import dataclass, field
import os

from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class Mem0DeepSeekLLMProvider:
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'llm': {
                'provider': 'takes from env', 
                'config': {
                    'model': 'deepseek-chat',
                    'temperature': 0.2,
                    'max_tokens': 2000,
                    'top_p': 1.0
                }
            }
        })
    
    def __post_init__(self):
        self._log_warnings()
        self.config['llm']['provider'] = os.environ.get('MEMORY_LLM_PROVIDER', 'deepseek')
        
        self.memory_llm_api_key = os.environ.get('DEEPSEEK_API_KEY')
        self.memory_llm_temperature = float(str(os.environ.get('MEMORY_LLM_TEMPERATURE', 0.2)))
        self.memory_llm_max_tokens = int(str(os.environ.get('MEMORY_LLM_MAX_TOKENS', 2000)))
        self.memory_llm_top_p = float(str(os.environ.get('MEMORY_LLM_TOP_P', 1.0)))
        
        self.config['llm']['config']['temperature'] = float(self.memory_llm_temperature)
        self.config['llm']['config']['max_tokens'] = int(self.memory_llm_max_tokens)
        self.config['llm']['config']['top_p'] = float(self.memory_llm_top_p)
    
    def _log_warnings(self):
        if os.environ.get('DEEPSEEK_API_KEY') is None:
            logger.warning('DeepSeek API key not found. Please set the DEEPSEEK_API_KEY environment variable.')


@dataclass
class Mem0OpenaiLLMProvider:
    '''https://docs.mem0.ai/components/llms/models/openai'''
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'llm': {
                'provider': 'takes from env', 
                'config': {
                    'model': 'takes from env',
                    'temperature': 0.2,
                    'max_tokens': 2000,
                    'top_p': 1.0
                }
            }
        })
    
    def __post_init__(self):
        self._log_warnings()
        self.config['llm']['provider'] = os.environ.get('MEMORY_LLM_PROVIDER', 'openai')
        
        self.memory_llm_api_key = os.environ.get('OPENAI_API_KEY')
        self.memory_llm_openai_model = os.environ.get('MEMORY_LLM_OPENAI_MODEL', 'gpt-4.1-nano-2025-04-14')
        self.memory_llm_temperature = float(str(os.environ.get('MEMORY_LLM_TEMPERATURE', 0.2)))
        self.memory_llm_max_tokens = int(str(os.environ.get('MEMORY_LLM_MAX_TOKENS', 2000)))
        self.memory_llm_top_p = float(str(os.environ.get('MEMORY_LLM_TOP_P', 1.0)))
        
        self.config['llm']['config']['model'] = self.memory_llm_openai_model        
        self.config['llm']['config']['temperature'] = float(self.memory_llm_temperature)
        self.config['llm']['config']['max_tokens'] = int(self.memory_llm_max_tokens)
        self.config['llm']['config']['top_p'] = float(self.memory_llm_top_p)
    
    def _log_warnings(self):
        if os.environ.get('OPENAI_API_KEY') is None:
            logger.warning('OPENAI_API_KEY key not found. Please set the OPENAI_API_KEY environment variable.')
    