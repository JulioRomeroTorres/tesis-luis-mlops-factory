import time
import ast
import requests
from pydantic import BaseModel
import google.oauth2.id_token
import google.auth.transport.requests
from itertools import cycle
from typing import Dict, TypeVar, Type, Optional, Union, Any, List
from mlops_pm25.pipelines.batch_process.repository.http_client import HttpClient
from mlops_pm25.pipelines.batch_process.constants import DEFAULT_EXPIRATION_TIME, DEFAULT_ENTITIES_EXTRACTION_MESSAGE
from mlops_pm25.pipelines.batch_process.constants import CORE_AGENT_API, AGENT_ENDPOINT, MAX_RETRIES
from mlops_pm25.pipelines.batch_process.repository.retry_manager import retry_manager

T = TypeVar("T", bound=BaseModel)

class WorkFlowAgent(HttpClient):
    _session_pool = None
    _session_cycle = None
    _failed_invocations = []

    def __init__(self, file: str, process_id: str, additional_files: Optional[List[str]] = []):
        super().__init__(CORE_AGENT_API)

        self.process_id = process_id
        self.file = file
        self.token = None
        self.additional_files = additional_files
        self.expiration_token_time = DEFAULT_EXPIRATION_TIME
        self._session = next(self._session_cycle) if self._session_cycle else requests.Session()

    @classmethod
    def initialize_pool(cls, pool_size: int = 10):
        cls._session_pool = [requests.Session() for _ in range(pool_size)]
        cls._session_cycle = cycle(cls._session_pool)

    @classmethod
    def close_pool(cls):
        if cls._session_pool:
            for session in cls._session_pool:
                session.close()
            cls._session_pool = None
            cls._session_cycle = None 

    def _get_bearer_token(self): 
        request = google.auth.transport.requests.Request()
        target_audience = self.http_core_path
        id_token = google.oauth2.id_token.fetch_id_token(request, target_audience)
        return id_token
    
    def get_access_token(self):
        if self.token is None or time.time() > self.token_expiry:
            self.token = self._get_bearer_token()
            self.token_expiry = time.time() + self.expiration_token_time
       
    def _get_headers(self):

        self.get_access_token()

        return {
            "Authorization": f"Bearer {self._get_bearer_token()}",
            "Content-Type": "application/json",
        }

    @retry_manager.track_execution(max_retries=MAX_RETRIES)
    def execute_task(
        self
    ) -> Union[Dict[str, Any], T]:
        
        payload = {
            "process_id": self.process_id,
            "file_path": self.file,
            "additional_files": self.additional_files
        }
        try:
            with self._session.post(
                self._get_total_path(AGENT_ENDPOINT),
                json=payload,
                headers=self._get_headers()
            ) as response:
                response.raise_for_status()
                response_data = response.json()
                print("response_data", response_data)
                return response_data
        except Exception as error:
            print(f"Error in Http {error} in {payload}")
            raise error