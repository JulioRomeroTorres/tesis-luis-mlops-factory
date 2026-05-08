from app.domain.exceptions import DomainExceptionCode, DomainException, AgentNotFound, ThreadNotFound, GuardialError
from app.presentation.api.exceptions import HttpApiException
from http import HTTPStatus

class ExceptionMapper:

    _DOMAIN_TO_HTTP = {
        DomainExceptionCode.AGENT_NOT_FOUND: HTTPStatus.NOT_FOUND,
        DomainExceptionCode.GUARDIAL_POLICIES_VIOLATED: HTTPStatus.BAD_REQUEST,
        DomainExceptionCode.THREAD_NOT_FOUND: HTTPStatus.NOT_FOUND,
        DomainExceptionCode.EXISTING_RESOURCE: HTTPStatus.UNPROCESSABLE_ENTITY
    }
    
    @classmethod
    def map_domain_to_http(cls, domain_exception: DomainException) -> HttpApiException:
    
        status_code = cls._DOMAIN_TO_HTTP.get(
            domain_exception.error_code, 
            HTTPStatus.INTERNAL_SERVER_ERROR
        )
                
        return HttpApiException(
            message=domain_exception.message,
            status_code=status_code,
            error_code=domain_exception.error_code,
            payload=domain_exception.format_respone()
        )