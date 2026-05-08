import os
import csv
import logging
import fitz
from PIL import Image
import io
from collections import defaultdict
from typing import Any, Dict, List, Optional, Union, Tuple
from uuid import uuid4, UUID
from datetime import datetime, timezone
from urllib.parse import urlparse
from app.domain.contants import MEDIA_FILE_MAPPER, DEFAULT_DT_FORMAT
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm 
from agent_framework import Content
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

def secuential_pdf_to_img(img_folder: str, source_pdf: str, dpi: Optional[int] = 150, format: Optional[str] = 'jpg'):

    os.makedirs(img_folder, exist_ok=True)

    pdf = fitz.open(source_pdf)
    
    for page_number in range(len(pdf)):
        pagina = pdf[page_number]
        
        zoom = dpi / 72
        matriz = fitz.Matrix(zoom, zoom)
        pixmap = pagina.get_pixmap(matrix=matriz)
        
        img_data = pixmap.tobytes("ppm")
        img = Image.open(io.BytesIO(img_data))
        
        file_name = f"page_{page_number + 1:03d}.{format}"
        ruta_completa = os.path.join(img_folder, file_name)
        
        if format.lower() in ["jpg", "jpeg"]:
            if img.mode in ("RGBA", "LA", "P"):
                img = img.convert("RGB")
            img.save(ruta_completa, "JPEG", quality=95)
        else:
            img.save(ruta_completa, "PNG")
        
    pdf.close()

def page_pdf_to_img(
                source_pdf: str, page_number: str, output_directory: str,
                output_file_path: str, format: Optional[str] = 'jpg', dpi: Optional[int] = 150 
            ) -> Tuple[int, str]:
    os.makedirs(output_directory, exist_ok=True)
    pdf = fitz.open(source_pdf)
    pagina = pdf[page_number]
        
    zoom = dpi / 72
    matriz = fitz.Matrix(zoom, zoom)
    pixmap = pagina.get_pixmap(matrix=matriz)
    
    img_data = pixmap.tobytes("ppm")
    img = Image.open(io.BytesIO(img_data))
    
    if format.lower() in ["jpg", "jpeg"]:
        if img.mode in ("RGBA", "LA", "P"):
            img = img.convert("RGB")
        img.save(output_file_path, "JPEG", quality=95)
    else:
        img.save(output_file_path, "PNG")
    
    pdf.close()
    return page_number, output_file_path


def parrallel_pdf_to_img(source_pdf: str, dpi: Optional[int] = 150, 
                         image_format: Optional[str] = 'jpg', max_workers:  Optional[int] = 40) -> List[str]:
    print(f"Source Pdf {source_pdf}")
    source_pdf_file = fitz.open(source_pdf)
    total_pages = len(source_pdf_file)
    file_name = source_pdf_file.name
    source_pdf_file.close()

    output_directory_name = f"images/{generate_uuid()}/{file_name}"

    result_process = []

    args_list = [
        (
            source_pdf, index, output_directory_name, 
            f"{output_directory_name}/page_{index}.{image_format}", image_format, dpi
        ) 
        for index in range(total_pages)
    ]
    
    
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
                    executor.submit(page_pdf_to_img, *args) 
                    for args in args_list
                ]
        
        with tqdm(total=total_pages, desc="Converting pdf to images", unit="pag") as pbar:
            for future in as_completed(futures):
                result_process.append(future.result())
                pbar.update(1)

    result_process = [ result for _, result in sorted(result_process, key=lambda x: x[0]) ]

    return result_process

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

def url_to_data_content(url: str, media_type: str) -> Any:
    import requests
    import base64
    print(f"La url {url}")
    response = requests.get(url)
    response.raise_for_status()
    
    base64_data = base64.b64encode(response.content).decode('utf-8')
    
    data_uri = f"data:{media_type};base64,{base64_data}"
    
    return Content.from_uri(data_uri)
    
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