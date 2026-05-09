from typing import Dict, Any
from http import HTTPStatus

from app.domain.exceptions import DomainExceptionCode

class HttpApiException(Exception):
    def __init__(
        self, 
        message: str, 
        status_code: int = HTTPStatus.BAD_GATEWAY,
        error_code: DomainExceptionCode = DomainExceptionCode.GUARDIAL_POLICIES_VIOLATED,
        payload: Dict[str, Any] = None
    ):
        super().__init__(message)
        self.message = message
        self.status_code = status_code
        self.error_code = error_code
        self.payload = payload or {}

    def format_json(self) -> Dict[str, Any]:
        return {
            "internal_code": self.error_code.value,
            "message": self.message,
            "details": self.payload
        }

