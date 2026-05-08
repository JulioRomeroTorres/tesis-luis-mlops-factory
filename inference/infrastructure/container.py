from typing import Any
from functools import lru_cache
from app.config import get_settings

from app.application.use_cases.handle_inference import (
    HandleInferenceUseCase
)

from app.infrastructure.repository.mongo_db import FirestoreRepository
from app.infrastructure.repository.storage_account import StorageAccountRepository


from pymongo import AsyncMongoClient
from app.infrastructure.managers.http_manager import HttpRepositoryManager

class DependencyContainer:
    def __init__(self):
        self._instances = {}
        self._factories = {}
        self._initialized = False
        self._db_client = None
        self._storage_client = None
        self._content_safety_client = None
        self._queue_client = None

    def _ensure_initialized(self):
        if self._initialized:
            return

        settings = get_settings()

        self._factories["db_repository"] = lambda: FirestoreRepository(self._get_db_client(), settings.mongo_db_name)
        self._factories["storage_repository"] = lambda: StorageAccountRepository(self._get_storage_client(), settings.storage_account_name)


        self._factories["inference_manager"] = lambda: InferenceManager(
            self.get('db_repository'),
        )

        self._initialized = True

    def get(self, service_name: str) -> Any:
        self._ensure_initialized()

        if service_name not in self._instances:
            if service_name not in self._factories:
                raise ValueError(f"Unknown service: {service_name}")
            self._instances[service_name] = self._factories[service_name]()

        return self._instances[service_name]
    
    def get_handle_inference_use_case(self) -> HandleInferenceUseCase:
        return HandleInferenceUseCase(
            inference_manager=self.get("inference_manager")
        )

    def clear(self):
        self._instances.clear()
        self._initialized = False
    
    def _get_db_client(self) -> AsyncMongoClient:
        if self._db_client is None:
            settings = get_settings()
            self._db_client = AsyncMongoClient(settings.mongo_db_connection_string)
        return self._db_client

    async def close_all(self):
        
        print("Closing all connection...")
        if self._db_client:
            await self._db_client.close()

        await HttpRepositoryManager.close_all_sessions()

        self.clear()
        print("All connection are closed")

@lru_cache
def get_container() -> DependencyContainer:
    return DependencyContainer()
