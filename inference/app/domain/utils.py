import os
import csv
import logging
import io
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union, Tuple
from uuid import uuid4, UUID
from datetime import datetime, timezone
from urllib.parse import urlparse
from app.domain.contants import MEDIA_FILE_MAPPER, DEFAULT_DT_FORMAT
from concurrent.futures import ProcessPoolExecutor, as_completed
from app.config import get_settings

logger = logging.getLogger(__name__)

def generate_uuid() -> UUID:
    return uuid4()

def filter_unnecesary_keys_from_dict(data: Dict[str, Any], valid_keys: List[str])-> Dict[str, Any]:
    return { f"{key}": data[key]  for key in data.keys() if key in valid_keys }

def replace_path_param(raw_value: str, mapper_value: Dict[str, str]) -> str:
    for key, value in mapper_value.items():
        raw_value = raw_value.replace(f"{{{key}}}", value)
    return raw_value

def get_or_create_uuid(session_id: Optional[Union[UUID, str]] =  None) -> UUID:
    if session_id is None:
        return generate_uuid()

    if isinstance(session_id, UUID):
        return session_id

    try:
        return UUID(session_id)
    except (ValueError, AttributeError):
        return generate_uuid()
    
def get_current_datetime() -> str:
    return datetime.now().isoformat()

def format_datetime_to_str(current_datetime: Optional[datetime] = None, format: Optional[str] = DEFAULT_DT_FORMAT):
    if current_datetime is None:
        return current_datetime
    return current_datetime.strftime(format)

def get_datetime_now() -> datetime:
    return datetime.now(timezone.utc)

def get_metadata_from_uri(url: str) -> Dict[str, str]:

    parsed_url = urlparse(url)
    path = parsed_url.path

    _, extension = os.path.splitext(path)
    type_file = extension.replace('.', '')

    mapped_media_file = MEDIA_FILE_MAPPER.get(type_file, None)
    if not mapped_media_file:
        raise TypeError(f"Type file {type_file} not found")
    
    return {
        "uri": url,
        "media_type": mapped_media_file
    }

def get_class_name(current_class: Any):
    return type(current_class).__name__
    
def grouped_by_key(information_list: List[Dict[str, Any]], 
                   groupped_key_name: str, order_key_name: Optional[str] = None) -> Dict[str, List[Any]]: 

    sorted_list = information_list if order_key_name is None else \
                    sorted(information_list, key=lambda x: x[order_key_name])
    
    groups = defaultdict(list)

    for element in sorted_list:
        if element[groupped_key_name] not in groups:
            groups[element[groupped_key_name]] = []
        groups[element[groupped_key_name]].append(element)

    return groups

def process_csv_to_jsonl(csv_content: bytes) -> List[Dict[str, Any]]:
    csv_text = csv_content.decode('utf-8')
    csv_reader = csv.DictReader(io.StringIO(csv_text))
    return [row for row in csv_reader]

def create_agent_webhook(agent_id: str) -> str:
    return f"{get_settings().app_host_url}/api/v1/agents/{agent_id}/question/"

def parse_comma_separated(value: str, field_name: str) -> List[str]:
    if not value or value.strip() == "":
        raise ValueError(f'{field_name} no puede estar vacío')
    import re
    pattern = r'^[^,]+(,[^,]+)*$'
    
    if not re.match(pattern, value):
        raise ValueError(
            f'{field_name} tiene formato inválido. '
            f'Debe ser IDs separados por coma (ej: "id1,id2,id3"). '
            f'No se permiten comas al inicio/final ni comas dobles. '
            f'Recibido: "{value}"'
        )
    
    elements = [ separated_element.strip() for separated_element in value.split(",") ]

    if any(not element for element in elements):
        raise ValueError(f'{field_name} contiene IDs vacíos')
    
    return elements

def get_full_name_agent(agent_id: str, pool_agent_informatio):
    print("ajaaa", pool_agent_informatio)
    selected_agent = pool_agent_informatio.get(agent_id)
    print("selected_agent", selected_agent)
    return f"{selected_agent.get("name")}:{selected_agent.get("version")}"