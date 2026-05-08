from typing import Optional
from typing import Any, Dict, List
JsonType = Dict[str, Any]
JsonArrayType = List[JsonType]

class INotSqlRepository:
    
    def _get_container(self, container_name: str):
        pass

    def _create_container(self, container_name: str):
        pass

    def get_item(self, item_id: str, partition_key: str, container: Optional[str] = None):
        pass

    def insert_item(self, raw_data: JsonType, container: Optional[str] = None) -> None:
        pass

    def query_items(self, query: str, parameters: Optional[JsonArrayType] = None, 
                    enable_cross_partition_query: Optional[bool] = None,
                    container: Optional[str] = None):
        pass

    def delete_item(self):
        pass

    def upsert_item(self, body: Dict[str, Any], container: Optional[str] = None) -> None:
        pass

    def update_item(self, item_id: str, partition_key: str, container: Optional[str] = None):
        pass