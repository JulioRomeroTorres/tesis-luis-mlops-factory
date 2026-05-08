import logging
import sys
import uvicorn

from app.config import get_settings

from app.presentation.api.dependencies import (
    get_container,
)

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware

from app.presentation.api.exceptions import HttpApiException
from app.domain.exceptions import DomainException

from pydantic import ValidationError
from app.presentation.exception_handlers import api_exception_handler, domain_exception_handler, generic_exception_handler
from app.presentation.exception_handlers import request_validation_exception_handler, validation_exception_handler
from app.presentation.api.routes import (
    prediction, checks
)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.getLogger('app.infrastructure').setLevel(logging.INFO)
    logging.getLogger('app.presentation').setLevel(logging.INFO)

@asynccontextmanager
async def lifespan(app: FastAPI):

    try:
        print("✅ Application is running...")
        yield        
    finally:
        print("🛑 Shutting down application...")
        container = get_container()
        await container.close_all()
        print("✅ All connections closed")
    

def create_app():
    app = FastAPI(debug=False, lifespan=lifespan)
    
    app.include_router(prediction.router)
    app.include_router(checks.router)
    
    settings = get_settings()

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "OPTIONS", "PUT", "PATCH"],
        allow_headers=["*"],
        expose_headers=["Content-Type", "Cache-Control"]
    )

    app.add_exception_handler(Exception, generic_exception_handler)
    app.add_exception_handler(RequestValidationError, request_validation_exception_handler)
    app.add_exception_handler(ValidationError, validation_exception_handler)
    app.add_exception_handler(DomainException, domain_exception_handler)
    app.add_exception_handler(HttpApiException, api_exception_handler)

    return app, settings

if __name__ == "__main__":
    app, _ = create_app()
    uvicorn.run(app, host="0.0.0.0", port=8080)
