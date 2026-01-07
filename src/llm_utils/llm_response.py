from typing import Dict, Any
from time import perf_counter
import os

from mem0 import Memory  # type: ignore

from log.logger import get_logger
from environment import Config
from .prompt_example import SYSTEM_PROMPT
from configs.chat_llm_providers import *
from redis_memory import RedisMemory
from kafka_bus.communication import Orchestrator
from kafka_bus.consumer import KafkaConsumerOutput

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
        self.search_top_k = int(os.environ.get('MEMORY_SEARCH_TOP_K', 5))
        self.search_score_threshold = float(os.environ.get('MEMORY_SEARCH_SCORE_THRESHOLD', 0.2))
        self.max_context_chars = int(os.environ.get('MEMORY_CONTEXT_MAX_CHARS', 2000))
        self.default_user_id = os.environ.get('MEMORY_DEFAULT_USER_ID', 'default')
        self.default_session_id = os.environ.get('MEMORY_DEFAULT_SESSION_ID', 'default_session')
        self.chat_client = self._define_chat_client()
        self.redis_memory = RedisMemory(
            host=self.cfg.REDIS.host,
            port=self.cfg.REDIS.port,
            db=self.cfg.REDIS.db,
            window_size=self.cfg.REDIS.window_size,
        )
        
        
    def get_response(self, msg: KafkaConsumerOutput) -> str | None:

        request_text: str = msg.request_text
        request_id: str = msg.request_id
        bot_id: str = msg.bot_id
        chat_id: str = msg.chat_id
        natal_chart: str = msg.natal_chart

        if self.chat_client is None:
            return ''
        chat_id = self._chat_id(chat_id, bot_id)
        search_t0 = perf_counter()
        recent_dialog = self._load_recent_dialog(chat_id)
        memory_context = self._search_memory_context(request_text, chat_id)
        search_t1 = perf_counter()

        messages = self._build_messages(request_text, natal_chart, recent_dialog, memory_context)
        logger.debug(f'Request messages: {messages}')

        response_content = self._chat_completion(messages)
        if response_content is None:
            return ''

        dialog_pair = [
            {'role': 'user', 'content': request_text},
            {'role': 'assistant', 'content': response_content},
        ]

        save_t0 = perf_counter()
        self._persist_memory(dialog_pair, chat_id)
        save_t1 = perf_counter()

        logger.info(
            'Timings: search=%.2f sec, chat+save=%.2f sec',
            search_t1 - search_t0,
            save_t1 - search_t1,
        )

        return response_content

    def _load_recent_dialog(self, chat_id: str) -> list[Dict[str, str]]:
        try:
            return self.redis_memory.get_last_messages(chat_id)
        except Exception as redis_err:
            logger.warning('Redis history fetch failed: %s', redis_err, exc_info=True)
            return []

    def _search_memory_context(self, request_text: str, chat_id: str) -> str:
        try:
            search_results = self.memory.search(request_text, user_id=chat_id)
            logger.info('Mem0 search results: %s', search_results)

            items = []
            if isinstance(search_results, dict):
                items = search_results.get('results') or search_results.get('data') or []
            elif isinstance(search_results, list):
                items = search_results

            items = list(items) if items else []

            if not items and chat_id != self.default_user_id:
                fallback_results = self.memory.search(request_text, user_id=self.default_user_id)
                logger.info('Mem0 fallback search results: %s', fallback_results)
                if isinstance(fallback_results, dict):
                    items = fallback_results.get('results') or fallback_results.get('data') or []
                elif isinstance(fallback_results, list):
                    items = fallback_results or []
                else:
                    items = []

            if items:
                return self._build_context(items)
        except Exception as mem_err:
            logger.warning('Mem0 search failed: %s', mem_err, exc_info=True)

        return ''

    def _build_messages(
        self,
        request_text: str,
        natal_chart: str,
        recent_dialog: list[Dict[str, str]],
        memory_context: str,
    ) -> list[Dict[str, str]]:
        messages: list[Dict[str, str]] = [{'role': 'system', 'content': SYSTEM_PROMPT}]
        if recent_dialog:
            messages.extend(recent_dialog)
        if memory_context:
            messages.append(
                {
                    'role': 'system',
                    'content': f'--- CONTEXT START ---\n{memory_context}\n--- CONTEXT END ---',
                }
            )
        if natal_chart:
            messages.append(
                {
                    'role': 'system',
                    'content': f'--- NATAL CHART START ---\n{natal_chart}\n--- NATAL CHART END ---',
                }
            )
        messages.append({'role': 'user', 'content': request_text})
        return messages

    def _chat_completion(self, messages: list[Dict[str, str]]) -> str | None:
        try:
            response_t0 = perf_counter()
            response = self.chat_client.chat.completions.create(
                model=self.cfg.CHAT_LLM.config['llm']['config']['model'],
                messages=messages,
            )
            logger.info(response)
            response_content = response.choices[0].message.content
            response_t1 = perf_counter()
            logger.info(response_content)
            logger.info('Chat time: %.2f sec', response_t1 - response_t0)
            return response_content
        except Exception as chat_err:
            logger.error(f'Failed to get response from OpenAI: {chat_err}', exc_info=True)
            return None

    def _persist_memory(
        self,
        dialog_pair: list[Dict[str, str]],
        chat_id: str,
    ) -> None:  
        try:
            self.memory.add(dialog_pair, user_id=chat_id, metadata={'role': 'conversation'})
        except Exception as mem_add_err:
            logger.warning('Mem0 add failed: %s', mem_add_err, exc_info=True)

        try:
            self.redis_memory.add_message(chat_id, 'user', dialog_pair[0]['content'])
            self.redis_memory.add_message(chat_id, 'assistant', dialog_pair[1]['content'])
        except Exception as redis_err:
            logger.warning('Redis history save failed: %s', redis_err, exc_info=True)

    def _format_memory_context(self, items: list[Dict[str, Any]]) -> str:
        if not items:
            return ''

        lines: list[str] = []
        total_len = 0
        for item in items:
            content = item.get('content', '').replace('\n', ' ').strip()
            if not content:
                continue

            score = item.get('score')
            score_str = f'{score:.2f}' if isinstance(score, (int, float)) else 'n/a'

            meta = item.get('metadata') or {}
            role = meta.get('role') or meta.get('source') or 'mem0'
            line = f'- [score {score_str}][role {role}] {content}'
            projected_len = total_len + len(line)

            lines.append(line)
            total_len = projected_len

            if total_len >= self.max_context_chars:
                break

        context_text = '\n'.join(lines)
        return context_text[: self.max_context_chars]

    def _build_context(self, items: list[Any]) -> str:
        sorted_items = sorted(
            [it for it in items if isinstance(it, dict)],
            key=lambda x: self._score_value(x),
            reverse=True,
        )
        logger.info(
            'Mem0 candidates (sorted): %s',
            [
                {
                    'score': it.get('score'),
                    'rerank_score': it.get('rerank_score'),
                    'content': (it.get('memory') or it.get('content') or '')[:80],
                }
                for it in sorted_items
            ],
        )

        unique_chunks = set()
        filtered_chunks = []
        top_k_items = sorted_items[: self.search_top_k]

        for item in top_k_items:
            score = item.get('score')
            content = item.get('memory') or item.get('content') or ''
            if not content:
                continue
            content = content.strip()
            if content in unique_chunks:
                continue
            if score is not None and score < self.search_score_threshold:
                continue

            unique_chunks.add(content)
            filtered_chunks.append(
                {
                    'content': content,
                    'score': score,
                    'metadata': item.get('metadata') or {},
                }
            )

        if not filtered_chunks and top_k_items:
            best = top_k_items[0]
            content = (best.get('memory') or best.get('content') or '').strip()
            filtered_chunks.append(
                {
                    'content': content,
                    'score': best.get('score'),
                    'metadata': best.get('metadata') or {},
                }
            )
            logger.info('Mem0: no items passed threshold, used top-1 fallback')

        return self._format_memory_context(filtered_chunks)

    def _score_value(self, item: Dict[str, Any]) -> float:
        rerank = item.get('rerank_score')
        score = item.get('score')
        if rerank is not None:
            try:
                return float(rerank)
            except (TypeError, ValueError):
                pass
        if score is not None:
            try:
                return float(score)
            except (TypeError, ValueError):
                pass
        return 0.0

    def _chat_id(self, user_id: str, bot_id: str) -> str:
        return f'{user_id}:{bot_id}'
    
    def _define_chat_client(self):
        if isinstance(self.cfg.CHAT_LLM, ChatDeepSeekLLMProvider):
            from openai import OpenAI  # type: ignore
            chat_client = OpenAI(
                api_key=self.cfg.CHAT_LLM.chat_llm_api_key,
                base_url=self.cfg.CHAT_LLM.base_url
            )
            return chat_client
        elif isinstance(self.cfg.CHAT_LLM, ChatOpenaiLLMProvider):
            from openai import OpenAI  # type: ignore
            chat_client = OpenAI(
                api_key=self.cfg.CHAT_LLM.chat_llm_api_key,
            )
            return chat_client
        
        logger.error(f'Unknown chat LLM provider: {self.cfg.CHAT_LLM.__name__}')
        return