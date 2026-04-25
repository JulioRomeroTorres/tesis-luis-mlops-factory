import requests
from typing import Any, Optional, Dict
from threading import Lock

class HttpClient:

    def __init__(self, http_core_path: str):
        self.http_core_path = http_core_path
        self._session = requests.Session()
        self._token_lock = Lock()
        pass
    
    def _get_total_path(
        self,
        endpoint: str
    ) -> str:
        return f"{self.http_core_path}/{endpoint}"

    def post(
        self,
        endpoint: str, 
        payload: Dict[str, Any],
        headers: Optional[Dict[str, Any]] = None
    ):
        with self._session.post(self._get_total_path(endpoint), json=payload, headers=headers) as response:
            response.raise_for_status()
            return response.json()

    def get(
            self,
            endpoint: str,
            headers: Optional[Dict[str, Any]] = None
            ):
        response = requests.get(self._get_total_path(endpoint), headers=headers)
        return response.json()

    def close(self) -> None:
        self._session.close()