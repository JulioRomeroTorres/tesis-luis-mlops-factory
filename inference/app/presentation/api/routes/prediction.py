import logging
from fastapi import APIRouter
from starlette.responses import JSONResponse

from app.presentation.api.dependencies import (
    get_handle_inference_use_case
)

logger = logging.getLogger(__name__)

BASE_PATH = "/api/v1/prediction"

router = APIRouter(
    prefix=BASE_PATH
)

@router.get("/")
async def get_pm25_prediction(datetime_inference: str):
    handle_get_agents = get_handle_inference_use_case()
    inference_value = await handle_get_agents.get_inference(datetime_inference)
    return JSONResponse(inference_value.model_dump(), headers={"status_code": "200"})

@router.post("/comparation/")
async def create_agent(lower_limit_datetime_inference: str, upper_limit_datetime_inference: str):
    handle_get_agents = get_handle_inference_use_case()

    return JSONResponse({}, headers={"status_code": "200"})