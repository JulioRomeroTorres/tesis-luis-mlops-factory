from typing import Any
from functools import lru_cache
from app.config import get_settings

from app.application.use_cases.handle_inference import (
    HandleInferenceUseCase
)

from app.infrastructure.repository.firestore import FirestoreDbRepository
from app.application.services.inference_manager import InferenceManager


from google.cloud.firestore_v1 import AsyncClient

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

        self._factories["db_repository"] = lambda: FirestoreDbRepository(self._get_db_client())
        

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
    
    def _get_db_client(self) -> AsyncClient:
        if self._db_client is None:
            settings = get_settings()
            self._db_client = AsyncClient(database=settings.firestore_db_name)
        return self._db_client

    async def close_all(self):
        
        print("Closing all connection...")
        if self._db_client:
            await self._db_client.close()

        self.clear()
        print("All connection are closed")

@lru_cache
def get_container() -> DependencyContainer:
    return DependencyContainer()
