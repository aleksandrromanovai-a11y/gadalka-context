from typing import Dict, Any
from dataclasses import dataclass, field
import os

from log.logger import get_logger

logger = get_logger(__name__)


@dataclass
class ChatDeepSeekLLMProvider:
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
    
    base_url: str = 'https://api.deepseek.com'
    
    def __post_init__(self):
        self._log_warnings()
        self.chat_llm_api_key = os.environ.get('CHAT_LLM_API_KEY')
        self.chat_llm_temperature = float(str(os.environ.get('CHAT_LLM_TEMPERATURE', 0.2)))
        self.chat_llm_max_tokens = int(str(os.environ.get('CHAT_LLM_MAX_TOKENS', 2000)))
        self.chat_llm_top_p = float(str(os.environ.get('CHAT_LLM_TOP_P', 1.0)))
        
        self.config['llm']['config']['temperature'] = float(self.chat_llm_temperature)
        self.config['llm']['config']['max_tokens'] = int(self.chat_llm_max_tokens)
        self.config['llm']['config']['top_p'] = float(self.chat_llm_top_p)
    
    def _log_warnings(self):
        if os.environ.get('CHAT_LLM_API_KEY') is None:
            logger.warning('Chat LLM API key not found. Please set the CHAT_LLM_API_KEY environment variable.')
            

@dataclass
class ChatOpenaiLLMProvider:
    config: Dict[str, Any] = field(default_factory=lambda: 
        {
            'llm': {
                'provider': 'openai', 
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
        self.chat_llm_api_key = os.environ.get('OPENAI_API_KEY')
        self.chat_llm_openai_model = os.environ.get('CHAT_LLM_OPENAI_MODEL', 'gpt-4.1-nano-2025-04-14')
        
        self.chat_llm_temperature = float(str(os.environ.get('CHAT_LLM_TEMPERATURE', 0.2)))
        self.chat_llm_max_tokens = int(str(os.environ.get('CHAT_LLM_MAX_TOKENS', 2000)))
        self.chat_llm_top_p = float(str(os.environ.get('CHAT_LLM_TOP_P', 1.0)))
        
        self.config['llm']['config']['model'] = self.chat_llm_openai_model        
        self.config['llm']['config']['temperature'] = float(self.chat_llm_temperature)
        self.config['llm']['config']['max_tokens'] = int(self.chat_llm_max_tokens)
        self.config['llm']['config']['top_p'] = float(self.chat_llm_top_p)
        
    def _log_warnings(self):
        if os.environ.get('OPENAI_API_KEY') is None:
            logger.warning('OPENAI_API_KEY key not found. Please set the OPENAI_API_KEY environment variable.')
        if os.environ.get('CHAT_LLM_OPENAI_MODEL') is None:
            logger.warning('Chat LLM OpenAI model not found. Please set the CHAT_LLM_OPENAI_MODEL environment variable.')
            
    