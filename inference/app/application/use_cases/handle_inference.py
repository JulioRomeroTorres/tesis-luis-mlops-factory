import asyncio
from typing import List, Any, Dict
from app.application.services.inference_manager import InferenceManager
from app.domain.utils import get_dummy_date_range, get_information_from_dict
from app.domain.inference.pm25 import Pm25Inference, ComparationInference

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

    async def compare_prediction(self, lower_limit_datetime_inference: str, upper_limit_datetime_inference: str, station_id: str) -> List[ComparationInference]:

        predictions, measured_values = await asyncio.gather(
            self.inference_manager.get_predictions_from_range_date(station_id, lower_limit_datetime_inference, upper_limit_datetime_inference),
            self.inference_manager.get_measured_pm25_from_range_date(station_id, lower_limit_datetime_inference, upper_limit_datetime_inference)
        )

        dict_predictions = {f"{prediction["READING_DATETIME"].strftime("%d/%m/%Y%H:%M:")}" : prediction for prediction in predictions}
        dict_measured_values = {measured_value["READING_DATETIME"]: measured_value for measured_value in measured_values}

        date_range = get_dummy_date_range(lower_limit_datetime_inference, upper_limit_datetime_inference)

        return [
            ComparationInference(predicted=get_information_from_dict(dict_predictions, current_date, 'prediction'), measured=get_information_from_dict(dict_measured_values, current_date, 'N_PM25'), reading_datetime=current_date)
            for current_date in date_range
        ]
        