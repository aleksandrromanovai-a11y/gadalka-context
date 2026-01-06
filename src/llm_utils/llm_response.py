from typing import Dict, Any
from time import perf_counter
import os

from mem0 import Memory

from log.logger import get_logger
from environment import Config
from .prompt_example import USER_PROMPT, SYSTEM_PROMPT
from configs.chat_llm_providers import *

logger = get_logger(__name__)


class Responser:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        # Устанавливаем фиктивный OPENAI_API_KEY, если он не установлен или пустой
        # Это нужно, так как mem0 требует api_key для OpenAI-совместимого клиента, даже для локального TEI
        # Фиктивный ключ не будет использоваться, так как запросы идут на локальный TEI сервер через base_url
        openai_key = os.environ.get('OPENAI_API_KEY', '').strip()
        if not openai_key:
            os.environ['OPENAI_API_KEY'] = 'not-needed-for-local-tei'
            logger.info('Set dummy OPENAI_API_KEY for local TEI embeddings (not used for local server)')
        
        self.mem0_cfg = {
            'vector_store': cfg.MEM0.vector_store.config,
            'llm': cfg.MEM0.llm.config['llm'],
            'embedder': cfg.MEM0.embedder.config['embedder'],
            'reranker': cfg.MEM0.reranker.config['reranker']
        }
        
        self.memory = Memory.from_config(self.mem0_cfg)
        # logger.info(self.CFG.OPENAI_API_KEY)
        # self.openai_client = OpenAI(api_key=self.CFG.OPENAI_API_KEY)
        self.chat_client = self._define_chat_client()
        
        
    def get_response(self, prompt: str, metadata: Dict[str, Any]) -> str | None:
        if self.chat_client is None:
            return ''
        try:
            response_t0 = perf_counter()
            response = self.chat_client.chat.completions.create(
                    model=self.cfg.CHAT_LLM.config['llm']['config']['model'],
                    messages=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt}
                    ]
                )
            logger.info(response)
            response_content = response.choices[0].message.content
            response_t1 = perf_counter()
            logger.info(response_content)
            logger.info(f'Response time: {response_t1 - response_t0:.2f} sec')
            print(self.memory.search(prompt, user_id='matek'))
            # self.memory.add(prompt, user_id='matek')
            # self.memory.add(response_content, user_id='matek')
            current_messages = [
                {'role': 'user', 'content': prompt},
                {'role': 'assistant', 'content': response_content}
            ]
            self.memory.add(current_messages, user_id='matek')
            return response_content
        except Exception as e:
            logger.error(f'Failed to get response from OpenAI: {e}', exc_info=True)
            return ''
    
    def _define_chat_client(self):
        if isinstance(self.cfg.CHAT_LLM, ChatDeepSeekLLMProvider):
            from openai import OpenAI
            chat_client = OpenAI(
                api_key=self.cfg.CHAT_LLM.chat_llm_api_key,
                base_url=self.cfg.CHAT_LLM.base_url
            )
            return chat_client
        elif isinstance(self.cfg.CHAT_LLM, ChatOpenaiLLMProvider):
            from openai import OpenAI
            chat_client = OpenAI(
                api_key=self.cfg.CHAT_LLM.chat_llm_api_key,
            )
            return chat_client
        
        logger.error(f'Unknown chat LLM provider: {self.cfg.CHAT_LLM.__name__}')
        return