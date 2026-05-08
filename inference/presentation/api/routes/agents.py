import logging
from fastapi import APIRouter
from starlette.responses import JSONResponse

from app.presentation.api.dependencies import (
    get_handle_message_use_case,
    get_handle_message_stream_use_case,
    get_handle_threads_use_case,
    get_handle_agents_use_case
)

logger = logging.getLogger(__name__)

BASE_PATH = "/api/v1/prediction"

router = APIRouter(
    prefix=BASE_PATH
)

@router.get("/")
async def get_pm25_prediction(datetime_inference: str):
    handle_get_agents = get_handle_agents_use_case()
    agent_versions = await handle_get_agents.get_agent_version(agent_name)
    formatted_agents = [ agent_version.format_json() for agent_version in agent_versions ]
    return JSONResponse(formatted_agents, headers={"status_code": "200"})

@router.post("/comparation/")
async def create_agent(lower_limit_datetime_inference: str, upper_limit_datetime_inference: str):
    handle_get_agents = get_handle_agents_use_case()
    created_agent = await handle_get_agents.create_agent(user_id, agent_information_request)

    return JSONResponse(created_agent.format_json(), headers={"status_code": "200"})