from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List, Coroutine

JsonType = Dict[str, Any]
JsonArrayType = List[JsonType]

class IItemSqlRepository(ABC):

    @abstractmethod
    async def get_item_by_id(self, item_id: str, collection_name: Optional[str] = None) -> Coroutine[Any, Any, Optional[JsonType]]:
        pass

    @abstractmethod
    async def get_items_by_filter(self, filter: Dict[str, Any], projection: Optional[Dict[str, Any]] = None,
                                  collection_name: Optional[str] = None, length: Optional[int] = None) -> Coroutine[Any, Any, List[JsonType]]:
        pass

    @abstractmethod
    async def insert_item(self, raw_data: JsonType, collection_name: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def count_items(self, filter: Dict[str, Any], collection_name: Optional[str] = None) -> int:
        pass

    @abstractmethod
    async def aggregate(self, pipeline: JsonType, collection_name: Optional[str] = None, max_length: Optional[int] = None) -> JsonArrayType:
        pass

    @abstractmethod
    async def batch_insert(self, items: List[JsonType], collection_name: Optional[str] = None):
        pass

    @abstractmethod
    async def delete_item(self, item_id: str, collection_name: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def delete_many_items(self, filter: Dict[str, Any], collection_name: Optional[str] = None) -> None:
        pass

    @abstractmethod
    async def update_by_filter(self, filter: Dict[str, Any], updated_value: Dict[str, Any], collection_name: Optional[str] = None) -> None:
        pass