import uuid
from decimal import Decimal
from typing import List, Any, Dict
import pandas as pd
import re

def generate_uuid():
    return str(uuid.uuid4())

def get_file_name(path_file: str) -> str:
    return path_file.split("/")[-1]

def get_type_column(column):
    return type(column).__name__

def convert_to_decimal(value: float) -> Decimal:
    return Decimal(str(value)) if isinstance(value, (float, int)) else value

def normalize_numeric_value(row: Any, schema: List[Dict[str, str]]):
    numeric_fields = [f["name"] for f in schema if f.get("type") == "NUMERIC"]

    return {
        **row,
        **{
            field: convert_to_decimal(row[field])
            for field in numeric_fields 
            if field in row and isinstance(row[field], (float, int))
        }
    }

def is_null_value(value: Any) -> bool:
    if isinstance(value, (list, dict)):
        return False
    
    return pd.isna(value)

def convert_numeric_field(row_value, field_schema):
    field_schema_type = field_schema.get("type") 
    field_schema_fields = field_schema.get("fields", [])
    field_schema_mode = field_schema.get("mode", None)

    if is_null_value(row_value):
        return row_value

    if field_schema_type == "STRING":
        return row_value

    if field_schema_type == "NUMERIC":
        return convert_to_decimal(row_value)

    if field_schema_type == "RECORD" and field_schema_mode is not "REPEATED":
        return normalize_numeric_value(
                row_value, 
                field_schema_fields
                )
    
    elif field_schema_mode == "REPEATED":
        return[
                normalize_numeric_value(item, field_schema_fields)
                for item in row_value
            ]

def normalize_columns(dataframe: pd.DataFrame, schema: Dict[str, str]) -> pd.DataFrame:
    for entitie_schema in schema:

        dataframe[entitie_schema["name"]] = dataframe[entitie_schema["name"]].apply(
                lambda x: convert_numeric_field(x, entitie_schema)
        )

    return dataframe

def print_ocr_agent_welcome():
    ocr_agent_welcome = """
        🤖═════════════════════════════════════════════════════════════════════════════════════════🤖
    
            █████╗  ██████╗ ███████╗███╗   ██╗████████╗███████╗     ██████╗  ██████╗██████╗ 
            ██╔══██╗██╔════╝ ██╔════╝████╗  ██║╚══██╔══╝██╔════╝    ██╔═══██╗██╔════╝██╔══██╗
            ███████║██║  ███╗█████╗  ██╔██╗ ██║   ██║   █████╗      ██║   ██║██║     ██████╔╝
            ██╔══██║██║   ██║██╔══╝  ██║╚██╗██║   ██║   ██╔══╝      ██║   ██║██║     ██╔══██╗
            ██║  ██║╚██████╔╝███████╗██║ ╚████║   ██║   ███████╗    ╚██████╔╝╚██████╗██║  ██║
            ╚═╝  ╚═╝ ╚═════╝ ╚══════╝╚═╝  ╚═══╝   ╚═╝   ╚══════╝     ╚═════╝  ╚═════╝╚═╝  ╚═╝
                                 █████╗ ██╗   ██╗███╗   ██╗ █████╗ 
                                ██╔══██╗██║   ██║████╗  ██║██╔══██╗
                                ███████║██║   ██║██╔██╗ ██║███████║
                                ██╔══██║██║   ██║██║╚██╗██║██╔══██║
                                ██║  ██║╚██████╔╝██║ ╚████║██║  ██║
                                ╚═╝  ╚═╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝  ╚═╝
    🤖═════════════════════════════════════════════════════════════════════════════════════════🤖
            """
    print(ocr_agent_welcome)

def print_ocr_agent_good_bye():
    ocr_agent_good_bye = """
        🤖═══════════════════════════════════════════════════════════🤖
                        ██████╗  ██████╗██████╗ 
                        ██╔═══██╗██╔════╝██╔══██╗
                        ██║   ██║██║     ██████╔╝
                        ██║   ██║██║     ██╔══██╗
                        ╚██████╔╝╚██████╗██║  ██║
                        ╚═════╝  ╚═════╝╚═╝  ╚═╝
        ███████╗██╗███╗   ██╗ █████╗ ██╗     ██╗███████╗ █████╗ ██████╗  ██████╗ 
        ██╔════╝██║████╗  ██║██╔══██╗██║     ██║╚══███╔╝██╔══██╗██╔══██╗██╔═══██╗
        █████╗  ██║██╔██╗ ██║███████║██║     ██║  ███╔╝ ███████║██║  ██║██║   ██║
        ██╔══╝  ██║██║╚██╗██║██╔══██║██║     ██║ ███╔╝  ██╔══██║██║  ██║██║   ██║
        ██║     ██║██║ ╚████║██║  ██║███████╗██║███████╗██║  ██║██████╔╝╚██████╔╝
        ╚═╝     ╚═╝╚═╝  ╚═══╝╚═╝  ╚═╝╚══════╝╚═╝╚══════╝╚═╝  ╚═╝╚═════╝  ╚═════╝
        
        🤖═══════════════════════════════════════════════════════════🤖
            """
    print(ocr_agent_good_bye)

def regex_batch_name(full_batch_name: str):
    match = re.search(r'Lote_(\d+)_Solicitud_(\d+)', full_batch_name)

    if match:
        return match.group(1), match.group(2)

    return "", ""

def regex_invoice_number(invoice_info: str)-> str:
    match = re.search(r'(\d+)_(\d+)_([A-Z]\d+)_(\d+)\.pdf', invoice_info)

    if match:
        return f"{match.group(3)}-{match.group(4)}"
    return ""

def get_document_info(path_file: str) -> Dict[str, str]:
    subdirectory_list = path_file.split('/')

    file_name = subdirectory_list[-1]
    full_batch_name = subdirectory_list[-2]
    batch_name, application_name = regex_batch_name(full_batch_name)

    raw_processed_date = subdirectory_list[-4]

    cutt_off_date = subdirectory_list[-3].replace(raw_processed_date, "").replace("_","")
    processed_date = raw_processed_date.replace("_","-")

    return {
        "file_name": file_name,
        "batch_name": batch_name,
        "application_name": application_name,
        "cutt_off_date": cutt_off_date,
        "processed_date": processed_date,
        "invoice_number": regex_invoice_number(file_name)
    }