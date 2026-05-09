from typing import List, Any, Dict
from app.application.services.inference_manager import InferenceManager
from app.domain.utils import generate_uuid, get_datetime_now, grouped_by_key
from app.domain.inference.pm25 import Pm25Inference

class HandleInferenceUseCase:
    
    def __init__(self, inference_manager: InferenceManager):
        self.inference_manager = inference_manager
        pass

    async def get_inference(self, selected_period: str, station_id: str) -> Pm25Inference:

        created_register = await self.inference_manager.get_inference(selected_period, station_id)
        return Pm25Inference(**{
            "value": created_register.get("N_PM25"),
            "reading_datetime": created_register.get("READING_DATETIME"),
            "station_id": created_register.get("STATION_ID")
        })

    async def compare_inference(self, selected_period: str) -> Pm25Inference:
        pass
        