"""
This is a boilerplate pipeline 'export_to_bigquery'
generated using Kedro 0.19.13
"""
from typing import Dict
from .constants import SUFFIX_ENTITY

def look_up_extracted_entity(entity_name: str, catalog):
    if entity_name not in catalog.list():
        raise ValueError(f"La entidad '{entity_name}' no está en el catálogo")
    
    return catalog.load(entity_name)

def get_entity_table_information(entity_name: str, entity_dict: Dict[str, str]):
    entity_name = entity_name.replace(SUFFIX_ENTITY, '')
    
    if entity_name not in entity_dict.keys():
        raise ValueError(f"La entidad '{entity_name}' no tiene un esquema definido")

    table_name = entity_dict[entity_name].get('table_name')
    table_schema = entity_dict[entity_name].get('table_schema')

    return table_name, table_schema

