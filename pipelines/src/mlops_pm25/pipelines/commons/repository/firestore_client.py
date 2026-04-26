from datetime import timedelta
from google.cloud import firestore
from typing import List, Any, Dict, Optional
from mlops_pm25.pipelines.commons.domain.constants import DEFAULT_EXPIRATION_TIME

class FireStoreClient:
    def __init__(self, collection_name: Optional[str] = None):
        self.db_client = firestore.Client()
        self.collection_name = collection_name
        pass

    def _get_collection_name(self, collection_name: Optional[str] = None):
        return self.collection_name if collection_name is None else collection_name

    def insert_element(self, data: Dict[str, Any], collection_name: Optional[str] = None):
        collection_name = self._get_collection_name(collection_name)

        collection = self.db_client.collection(collection_name)
        new_document_reference = collection.document()
        new_document_reference.set(data)

        return new_document_reference

    def bulk_insert_elements(self, 
                             elements: List[Any],
                             batch_size: int = 100,
                             collection_name: Optional[str] = None
                             ):
        collection_name = self._get_collection_name(collection_name)

        bulk_writer = self.db_client.bulk_writer()
        coleccion = self.db_client.collection(collection_name)

        [
            bulk_writer.create(coleccion.document(), element)
            for element in elements
        ]    

        bulk_writer.flush()
    
        print(f"✅ Insertados {len(elements)} documentos")
        
    
    def get_elements_by_filters(self, 
                                filters: List[Any],
                                projections: Optional[List[str]] = None,
                                collection_name: Optional[str] = None) -> List[Dict[str, Any]]:
        collection_name = self._get_collection_name(collection_name)

        query = self.db_client.collection(collection_name)

        if projections:
            query = query.select(projections)

        for field, operator, value in filters:
            query = query.where(field, operator, value)
        
        resultados = query.stream()
        return [doc.to_dict() for doc in resultados]



