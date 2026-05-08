from typing import Optional, Dict, Any
from enum import Enum

class DomainExceptionCode(Enum):
    AGENT_NOT_FOUND = "AGENT_NOT_FOUND"
    THREAD_NOT_FOUND = "THREAD_NOT_FOUND"
    GUARDIAL_POLICIES_VIOLATED = "GUARDIAL POLICIES VIOLATED"
    SAVE_LOCALLY_ERROR = "SAVE_LOCALLY_ERROR"
    EXISTING_RESOURCE = "EXISTING_RESOURCE"

class DomainException(Exception):
    def __init__(self, message: str, error_code: Optional[DomainExceptionCode] = ""):
        self.message = message
        self.error_code = error_code
        super().__init__(message)

    def format_respone(self) -> Dict[str, Any]:
        pass

class ExistingResource(DomainException):
    def __init__(self, resource_name: str, table_name):
        super().__init__(f"El recurso {resource_name} ya existe", DomainExceptionCode.EXISTING_RESOURCE)
        self.resource_name = resource_name
        self.table_name = table_name

    def format_respone(self) -> Dict[str, Any]:
        return {
            "resource_name": self.resource_name,
            "table_name": self.table_name
        }

class AgentNotFound(DomainException):
    def __init__(self, agent_name: str):
        super().__init__(f"No existe el agente {agent_name}", DomainExceptionCode.AGENT_NOT_FOUND)
        self.agent_name = agent_name

    def format_respone(self) -> Dict[str, Any]:
        return {
            "agent_name": self.agent_name
        }
    
class GuardialError(DomainException):
    def __init__(self, question: str, thresholds_results: Dict[str, Any]):
        super().__init__(f"El agente no puede responder correctamente ya que contiene informacion no adeucada y se incumplen  {thresholds_results}",\
                        DomainExceptionCode.GUARDIAL_POLICIES_VIOLATED)
        self.thresholds_results = thresholds_results
        self.question = question
    
    def format_respone(self) -> Dict[str, Any]:
        return {
            "thresholds_results": self.thresholds_results,
            "question": self.question
        }

class ThreadNotFound(DomainException):
    def __init__(self, session_id: str):
        super().__init__(f"No hay threads para la siguiente sesion {session_id}", DomainExceptionCode.THREAD_NOT_FOUND)
        self.session_id = session_id
    
    def format_respone(self) -> Dict[str, Any]:
        return {
            "session_id": self.session_id
        }

class UploadDocumentError(DomainException):
    def __init__(self, file_name: str, error_description: str):
        super().__init__(f"{error_description}",\
                        DomainExceptionCode.SAVE_LOCALLY_ERROR)
        self.error_description = error_description
        self.file_name = file_name
    
    def format_respone(self) -> Dict[str, Any]:
        return {
            "file_name": self.file_name
        }
    