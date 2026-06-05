from typing import Optional
from datetime import datetime
from app.application.constants import CollectionInferenceEnum
from app.domain.repository.item_sql_repository import IItemSqlRepository
class InferenceManager:
    def __init__(self, db_repository: IItemSqlRepository):
        self.db_repository = db_repository
        pass

    async def get_inference(self, selected_period: str, station_id: str):
        collection_name = "pm25-inference"
        inference = await self.db_repository.get_items_by_filter(
            [
                ("READING_DATETIME", "==", selected_period),
                ("STATION_ID", "==", station_id)
            ],
            collection_name
        )
        return inference[0]

    async def get_variable_from_range_date(
            self, collection_name: str, station_id: str,
            lower_limit_datetime_inference: str, upper_limit_datetime_inference: str,
            date_column_name:Optional[str] = 'READING_DATETIME'
        ):

        lower_limit_datetime_inference = datetime.strptime(lower_limit_datetime_inference, "%d/%m/%Y%H:%M:")
        upper_limit_datetime_inference = datetime.strptime(upper_limit_datetime_inference, "%d/%m/%Y%H:%M:")

        inference = await self.db_repository.get_items_by_filter(
            [
                (date_column_name, ">=", lower_limit_datetime_inference),
                (date_column_name, "<=", upper_limit_datetime_inference),
                ("STATION_ID", "==", station_id)
            ],
            collection_name
        )
        print(f"Inference collected in collection_name {collection_name} {inference}")
        return inference
    
    async def get_predictions_from_range_date(
            self, station_id: str,
            lower_limit_datetime_inference: str, upper_limit_datetime_inference: str):
        predicted_values = await self.get_variable_from_range_date(CollectionInferenceEnum.INFERENCE_PM25.value, station_id, lower_limit_datetime_inference, upper_limit_datetime_inference)
        return predicted_values
    
    async def get_measured_pm25_from_range_date(
            self, station_id: str,
            lower_limit_datetime_inference: str, upper_limit_datetime_inference: str):
        measured_values = await self.get_variable_from_range_date(CollectionInferenceEnum.MEASURED_PM25.value, station_id, lower_limit_datetime_inference, upper_limit_datetime_inference, 'FORMATED_READING_DATETIME')
        return measured_values