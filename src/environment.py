from dataclasses import dataclass, field
import os


from configs.chat_llm_providers import ChatDeepSeekLLMProvider, ChatOpenaiLLMProvider
from configs.mem0_llm_providers import Mem0DeepSeekLLMProvider, Mem0OpenaiLLMProvider
from configs.mem0_vector_stores import QdrantStore
from configs.mem0_embedding_models import TEIDeepVkUserBgeM3, Mem0OpenaiEmbeddingModel
from configs.mem0_llm_rerankers import RerankerDeepSeekLLMProvider, RerankerOpenaiLLMProvider

from log.logger import get_logger

logger = get_logger(__name__)

@dataclass
class Mem0Config:
    llm: Mem0DeepSeekLLMProvider | Mem0OpenaiLLMProvider = field(default_factory=Mem0OpenaiLLMProvider)
    vector_store: QdrantStore = field(default_factory=QdrantStore)
    embedder: TEIDeepVkUserBgeM3 | Mem0OpenaiEmbeddingModel = field(default_factory=Mem0OpenaiEmbeddingModel)
    reranker: RerankerDeepSeekLLMProvider | RerankerOpenaiLLMProvider = field(default_factory=RerankerOpenaiLLMProvider)
    
    def __post_init__(self):
        # MEMORY_LLM_PROVIDER REDEFINE
        if os.environ.get('MEMORY_LLM_PROVIDER') is None:
            logger.error('MEMORY_LLM_PROVIDER environment variable not found. Please set the MEMORY_LLM_PROVIDER environment variable.')
            raise ValueError('MEMORY_LLM_PROVIDER environment variable not found. Please set the MEMORY_LLM_PROVIDER environment variable.')
        else:
            mem0_llm_map = {
                'deepseek': Mem0DeepSeekLLMProvider,
                'openai': Mem0OpenaiLLMProvider
            }
            try:
                llm_cls = mem0_llm_map[str(os.environ.get('MEMORY_LLM_PROVIDER'))]
                self.llm = llm_cls()
            except KeyError:
                logger.error(f'Unknown MEMORY_LLM_PROVIDER: {os.environ.get("MEMORY_LLM_PROVIDER")}')
                raise ValueError(f'Unknown MEMORY_LLM_PROVIDER: {os.environ.get("MEMORY_LLM_PROVIDER")}')
            
        # MEMORY_EMBEDDINGS_PROVIDER REDEFINE
        if os.environ.get('MEMORY_EMBEDDINGS_PROVIDER') is None:
            logger.error('MEMORY_EMBEDDINGS_PROVIDER environment variable not found. Please set the MEMORY_EMBEDDINGS_PROVIDER environment variable.')
            raise ValueError('MEMORY_EMBEDDINGS_PROVIDER environment variable not found. Please set the MEMORY_EMBEDDINGS_PROVIDER environment variable.')
        else:
            memory_embeddings_map = {
                'tei': TEIDeepVkUserBgeM3,
                'openai': Mem0OpenaiEmbeddingModel
            }
            try:
                embedder_cls = memory_embeddings_map[str(os.environ.get('MEMORY_EMBEDDINGS_PROVIDER'))]
                self.embedder = embedder_cls()
            except KeyError:
                logger.error(f'Unknown MEMORY_EMBEDDINGS_PROVIDER: {os.environ.get("MEMORY_EMBEDDINGS_PROVIDER")}')
                raise ValueError(f'Unknown MEMORY_EMBEDDINGS_PROVIDER: {os.environ.get("MEMORY_EMBEDDINGS_PROVIDER")}')
        
        # MEMORY_RERANKER_PROVIDER REDEFINE
        if os.environ.get('MEMORY_RERANKER_PROVIDER') is None:
            logger.error('MEMORY_RERANKER_PROVIDER environment variable not found. Please set the MEMORY_RERANKER_PROVIDER environment variable.')
            raise ValueError('MEMORY_RERANKER_PROVIDER environment variable not found. Please set the MEMORY_RERANKER_PROVIDER environment variable.')
        else:
            memory_reranker_map = {
                'openai': RerankerOpenaiLLMProvider,
                'deepseek': RerankerDeepSeekLLMProvider
            }
            try:
                reranker_cls = memory_reranker_map[str(os.environ.get('MEMORY_RERANKER_PROVIDER'))]
                self.reranker = reranker_cls()
            except KeyError:
                logger.error(f'Unknown MEMORY_RERANKER_PROVIDER: {os.environ.get("MEMORY_RERANKER_PROVIDER")}')
                raise ValueError(f'Unknown MEMORY_RERANKER_PROVIDER: {os.environ.get("MEMORY_RERANKER_PROVIDER")}')


@dataclass
class Config:
    CHAT_LLM: ChatDeepSeekLLMProvider | ChatOpenaiLLMProvider = field(default_factory=ChatOpenaiLLMProvider)
    MEM0: Mem0Config = field(default_factory=Mem0Config)
    
    def __post_init__(self):
        # CHAT_LLM REDEFINE
        if os.environ.get('CHAT_LLM_PROVIDER') is None:
            logger.error('CHAT_LLM_PROVIDER environment variable not found. Please set the CHAT_LLM_PROVIDER environment variable.')
            raise ValueError('CHAT_LLM_PROVIDER environment variable not found. Please set the CHAT_LLM_PROVIDER environment variable.')
        else:
            chat_llm_map = {
                'deepseek': ChatDeepSeekLLMProvider,
                'openai': ChatOpenaiLLMProvider
            }
            try:
                chat_llm_cls = chat_llm_map[str(os.environ.get('CHAT_LLM_PROVIDER'))]
                self.CHAT_LLM = chat_llm_cls()
            except KeyError:
                logger.error(f'Unknown CHAT_LLM_PROVIDER: {os.environ.get("CHAT_LLM_PROVIDER")}')
                raise ValueError(f'Unknown CHAT_LLM_PROVIDER: {os.environ.get("CHAT_LLM_PROVIDER")}')
            
    