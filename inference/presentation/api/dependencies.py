from app.application.use_cases.handle_inference import (
    HandleInferenceUseCase
)

from app.infrastructure.container import get_container

def get_handle_inference_use_case() -> HandleInferenceUseCase:
    return get_container().get_handle_inference_use_case()