from abc import ABC, abstractmethod
from typing import Optional, Any, Dict, List, Coroutine

JsonType = Dict[str, Any]
JsonArrayType = List[JsonType]

class IItemSqlRepository(ABC):

    @abstractmethod
    async def get_items_by_filter(self, filters: List[Any], collection_name: Optional[str] = None) -> JsonArrayType:
        pass