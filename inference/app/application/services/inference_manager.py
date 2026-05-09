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