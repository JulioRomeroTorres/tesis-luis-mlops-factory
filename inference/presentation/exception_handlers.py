import json
from fastapi.responses import JSONResponse
from app.presentation.api.exceptions import HttpApiException
from app.domain.exceptions import DomainException
from app.presentation.api.error_mapper import ExceptionMapper
from fastapi import status
from fastapi.exceptions import RequestValidationError
from pydantic import ValidationError
from lib.maf_telemetry.guardrails.middleware import SecurityViolation
from agent_framework.openai import OpenAIContentFilterException
from fastapi.logger import logger

async def domain_exception_handler(request, exc: DomainException):
    print("domain_exception_handler", exc)
    mapper_error = ExceptionMapper.map_domain_to_http(exc)
    return JSONResponse(mapper_error.format_json(), status_code=mapper_error.status_code)

async def api_exception_handler(request, exc: HttpApiException):
    logger.error(f"Api Error: {exc}") 
    return JSONResponse(exc.format_json(), status_code=exc.status_code)

async def security_exception_handler(request, exc: SecurityViolation):
    logger.error(f"Guardrails Error: {exc}")
    details_analysis = {"mode": exc.mode}
    details_analysis = details_analysis if exc.analysis_result is None else {"analysis":exc.analysis_result,**details_analysis}

    return JSONResponse({
        "message": exc.message,
        "internal_code": "GUARDRAIL_ERROR",
        "details": json.dumps(details_analysis)
    }, status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)

async def security_model_exception_handler(request, exc: OpenAIContentFilterException):
    return JSONResponse({
        "message": "El input no cumple la politica del guadrail del modelo de OpenAi",
        "internal_code": "GUARDRAIL_ERROR",
        "details": str(exc)
    }, status_code=status.HTTP_422_UNPROCESSABLE_CONTENT)

async def generic_exception_handler(request, exc: Exception):
    logger.error(f"Generic Error: {exc}") 

    return JSONResponse({
        "message": "Ocurrió un error inesperado en el servidor",
        "internal_code": "SERVER_ERROR",
        "details": str(exc)
    }, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)

async def request_validation_exception_handler(request, exc: RequestValidationError):
    logger.error(exc, exc_info=True)
    errors = [f'{x["loc"][0] if len(x["loc"]) == 1 else x["loc"]}: {x["msg"]}' for x in exc.errors()]
    return JSONResponse({
        'code': 'UNPROCESSABLE_ENTITY',
        'message': 'Error de validación en la petición',
        'details': errors,
    }, status_code=status.HTTP_422_UNPROCESSABLE_ENTITY)

async def validation_exception_handler(request, exc: ValidationError):
    logger.error(exc, exc_info=True)
    errors = [f'{x["loc"][0] if len(x["loc"]) == 1 else x["loc"]}: {x["msg"]}' for x in exc.errors()]
    return JSONResponse({
        'code': 'INTERNAL_VALIDATION_ERROR',
        'message': 'Validación interna incorrecta',
        'details': errors,
    }, status_code=status.HTTP_500_INTERNAL_SERVER_ERROR)