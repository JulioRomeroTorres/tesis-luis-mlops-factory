from google.cloud.firestore_v1 import AsyncClient, AsyncCollectionReference
from google.cloud.firestore_v1.base_query import FieldFilter
from typing import Optional, Any, Dict, List, Union
from app.domain.repository.item_sql_repository import IItemSqlRepository

JsonType = Dict[str, Any]
JsonArrayType = List[JsonType]

class FirestoreDbRepository(IItemSqlRepository):
    def __init__(self, client: AsyncClient, collection_name: Optional[str] = None) -> None:
        self.client = client
        self.collection = self._get_collection(collection_name) if collection_name else None

    def _get_collection(self, collection_name: str):
        return self.client.collection(collection_name)

    def _create_collection_reference(self, collection_name: str) -> AsyncCollectionReference:
        return self._get_collection(collection_name) if self.collection is None else self.collection

    async def get_items_by_filter(self, filters: List[Any], collection_name: Optional[str] = None) -> JsonArrayType:
        collection_name:AsyncCollectionReference = self._create_collection_reference(collection_name)
        try:
            query = collection_name
            for filter in filters:
                query = query.where(
                    filter=FieldFilter(filter[0], filter[1], filter[2])
                )

            response = []

            print("Fetching documents asynchronously...")
            async for doc in query.stream():
                response.append(doc.to_dict())
            return response

        except Exception as e:
            raise e
        