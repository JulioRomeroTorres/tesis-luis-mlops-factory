"""
This is a boilerplate pipeline 'commons'
generated using Kedro 0.19.13
"""
import pandas_gbq
import pandas as pd
from typing import List, Dict, Any, Optional
from .domain.schemas import MAPPER_AUNA_DOCS
from .utils import normalize_columns
from .domain.constants import BigQueryInsertionMode

def convert_json_to_df(json_entities: List[Dict[str, Any]]) -> pd.DataFrame:
    df_entities = pd.DataFrame(json_entities)
    print(df_entities.head())
    return df_entities

def save_prediction(
                    project_id: str, 
                    dataset: str,
                    table_name: str,
                    dataframe: pd.DataFrame,
                    schema: Optional[str]=None,
                    how_to_save: Optional[BigQueryInsertionMode]=BigQueryInsertionMode.APPEND.value
                    ) -> bool:
    
    if dataframe.empty:
        return False
    document_schema = MAPPER_AUNA_DOCS[schema]
    new_dataframe = normalize_columns(dataframe, document_schema)

    print(f"Bulk extracted entities in table {table_name}")
    print(f"Extrated Entities {new_dataframe.columns}")
    print(new_dataframe)

    correct_order_columns = [
        field["name"]
        for field in document_schema
    ]

    new_dataframe = new_dataframe[correct_order_columns]

    pandas_gbq.to_gbq(new_dataframe, f'{dataset}.{table_name}', project_id=project_id, 
                      if_exists=how_to_save, table_schema=document_schema)
    return True

def load_source_data(query: str, wait_previous_process: Optional[bool] = True):

    from kedro_datasets.pandas import GBQQueryDataset
    dataset = GBQQueryDataset(
        sql=query
    )

    source_data = dataset.load()
    print("LoadData", source_data.head())
    return source_data

def union_dataframes(first_df: pd.DataFrame, second_def: pd.DataFrame) -> pd.DataFrame:
    return pd.concat([first_df, second_def], axis=0, ignore_index=True)
