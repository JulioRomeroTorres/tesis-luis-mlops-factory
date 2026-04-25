import pandas as pd
from decimal import Decimal, ROUND_HALF_UP
from typing import List, Dict, Any, Optional
from datetime import datetime
from pathlib import Path
from jinja2 import Template
from mlops_pm25.pipelines.commons.domain.constants import ScaleNumber
from mlops_pm25.pipelines.commons.domain.constants import MAPPER_SCALE, DEFAULT_NUMERIC_SCALE

def get_type_column(column):
    return type(column).__name__

def convert_to_decimal(value: float, scale: Optional[int] =  DEFAULT_NUMERIC_SCALE) -> Decimal:
    return Decimal(str(value)).quantize( Decimal(f'1.{"0" * scale}'), rounding=ROUND_HALF_UP) if isinstance(value, (float, int)) else value

def normalize_numeric_value(row: Any, schema: List[Dict[str, str]]):
    numeric_fields = [f["name"] for f in schema if f.get("type") == "NUMERIC"]
    record_fields = [
        (
            f["name"], 
            {
                "fields": f["fields"], 
                "type": "RECORD"
            }
        )  for f in schema if f.get("type") == "RECORD" and (f.get("mode", None) is None)]

    record_array_fields = [
            (   
                f["name"], 
                {
                    "fields": f["fields"], 
                    "type": "RECORD", 
                    "mode": "REPEATED"
                }
            ) for f in schema if f.get("type") == "RECORD" and (f.get("mode", None) == "REPEATED")]

    return {
        **row,
        **{
            field: convert_to_decimal(row[field])
            for field in numeric_fields 
            if field in row and isinstance(row[field], (float, int))
        },
        **{
            field: convert_numeric_field(row[field], field_schema)
            for field, field_schema in record_fields 
            if field in row and isinstance(row[field], dict)
        },
        **{
            field: convert_numeric_field(row[field], field_schema)
            for field, field_schema in record_array_fields 
            if field in row and isinstance(row[field], list)
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
    print(f"row_value {row_value} field_schema_type: {field_schema_type} field_schema_fields {field_schema_fields}  field_schema_mode {field_schema_mode} ")
    if is_null_value(row_value):
        if field_schema_mode == "REPEATED":
            return []
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

def current_datetime():
    return datetime.now()

def string_date_to_datetime(current_string_date: str):
    return datetime.strptime(current_string_date, "%Y-%m-%d")

def replace_variables(template_str: str, mapper_values: Dict[str, str]):
    for key in mapper_values:
        template_str = template_str.replace(key, mapper_values[key])
    return template_str

def generate_query(input_file: Path, **replacements) -> str:

    with open(input_file, "r") as f:
        query_template = f.read()

    return Template(query_template).render(**replacements)


def is_empty_dataframe(current_df: pd.DataFrame) -> bool:
    return current_df.shape[0] == 0

def scale_number(value: float, scale: ScaleNumber, precision: float = 2) -> str:
    scaled_value = value/MAPPER_SCALE[scale]
    scaled_value = round(scaled_value, precision)

    return f"{scaled_value} {scale}"