from app.domain.repository.item_sql_repository import IItemSqlRepository
class InferenceManager:
    def __init__(self, db_repository: IItemSqlRepository):
        self.db_repository = db_repository
        pass

    async def get_inference(self, selected_period: str):
        collection_name = "pm25-inference"
        inference = await self.db_repository.get_items_by_filter(
            [("READING_DATETIME", "==", selected_period)],
            collection_name
        )
        return inference[0]