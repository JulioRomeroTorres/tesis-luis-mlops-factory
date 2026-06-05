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
async def get_pm25_prediction(datetime_inference: str, station_id: str):
    handle_get_agents = get_handle_inference_use_case()
    inference_value = await handle_get_agents.get_inference(datetime_inference, station_id)
    return JSONResponse(inference_value.format_json(), headers={"status_code": "200"})

@router.get("/comparation/")
async def compare_prediction(lower_limit_datetime_inference: str, upper_limit_datetime_inference: str, station_id: str):
    handle_get_agents = get_handle_inference_use_case()
    comparation_value = await handle_get_agents.compare_prediction(lower_limit_datetime_inference, upper_limit_datetime_inference, station_id)
    formatted_comparation = [ value.model_dump() for value in comparation_value]
    return JSONResponse(formatted_comparation, headers={"status_code": "200"})