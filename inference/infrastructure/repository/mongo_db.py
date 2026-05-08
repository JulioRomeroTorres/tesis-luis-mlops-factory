from bson import ObjectId
from pymongo import AsyncMongoClient
from typing import Optional, Any, Dict, List, Union
from app.domain.repository.item_sql_repository import IItemSqlRepository

JsonType = Dict[str, Any]
JsonArrayType = List[JsonType]

class MongoDbRepository(IItemSqlRepository):
    def __init__(self, client: AsyncMongoClient, database_name: str, collection_name: Optional[str] = None) -> None:
        self.client = client
        self.database = self.client[database_name]
        self.collection = self._get_collection(collection_name) if collection_name else None

    def _get_collection(self, collection_name: str):
        return self.database[collection_name]

    def _create_collection_reference(self, collection_name: str):
        return self._get_collection(collection_name) if self.collection is None else self.collection

    async def get_item_by_id(self, item_id: Union[ObjectId, str], collection_name: Optional[str] = None) -> Optional[JsonType]:
        object_id = ObjectId(item_id) if isinstance(item_id, str) else item_id
        collection = self._create_collection_reference(collection_name)
        document = await collection.find_one({"_id": object_id})
        return document

    async def get_items_by_filter(self, filter: Dict[str, Any], projection: Optional[Dict[str, Any]] = None,
                                  collection_name: Optional[str] = None, length: Optional[int] = None) -> List[JsonType]:
        collection = self._create_collection_reference(collection_name)
        return await collection.find(
            filter=filter,
            projection=projection
        ).to_list(length=length)

    async def insert_item(self, raw_data: JsonType, collection_name: Optional[str] = None) -> Any:
        collection = self._create_collection_reference(collection_name)
        return await collection.insert_one(raw_data)

    async def count_items(self, filter: Dict[str, Any], collection_name: Optional[str] = None) -> int:
        collection = self._create_collection_reference(collection_name)
        return await collection.count_documents(filter)

    async def aggregate(self, pipeline: JsonType, collection_name: Optional[str] = None, max_length: Optional[int] = None) -> JsonArrayType:
        collection = self._create_collection_reference(collection_name)
        filtered_items = await collection.aggregate(pipeline)
        return await filtered_items.to_list(length=max_length)

    async def batch_insert(self, items: List[JsonType], collection_name: Optional[str] = None):
        collection = self._create_collection_reference(collection_name)
        await collection.insert_many(items)

    async def delete_item(self, item_id: str, collection_name: Optional[str] = None) -> None:
        collection = self._create_collection_reference(collection_name)
        await collection.delete_one({"_id": ObjectId(item_id)})

    async def delete_many_items(self, filter: Dict[str, Any], collection_name: Optional[str] = None) -> None:
        collection = self._create_collection_reference(collection_name)
        await collection.delete_many(filter)

    async def update_by_filter(self, filter: Dict[str, Any], updated_value: Dict[str, Any], collection_name: Optional[str] = None) -> None:
        collection = self._create_collection_reference(collection_name)
        await collection.update_one(filter, {"$set": updated_value})
        