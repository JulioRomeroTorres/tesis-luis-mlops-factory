"""
This is a boilerplate pipeline 'commons'
generated using Kedro 0.19.13
"""
import pandas as pd
from typing import List, Dict, Any, Optional
from .domain.schemas import MAPPER_AUNA_DOCS
from .utils import (
    normalize_columns,
    create_lag_features, create_rolling_features,
    create_cyclic_features, create_temporal_features
) 
from .domain.constants import BigQueryInsertionMode, MAPPER_CYCLIC_VALUE
from mlops_pm25.pipelines.commons.repository.firestore_client import FireStoreClient

def convert_json_to_df(json_entities: List[Dict[str, Any]]) -> pd.DataFrame:
    df_entities = pd.DataFrame(json_entities)
    print(df_entities.head())
    return df_entities

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

def convert_type_columns(meteorological_df: pd.DataFrame):
    meteorological_df['READING_DATETIME'] = pd.to_datetime(meteorological_df['READING_DATETIME'], format='%d/%m/%Y%H:%M:')
    return meteorological_df

def create_cyclic_meteorological_features(meteorological_df: pd.DataFrame, datetime_column_name: str):
    merged_df, created_temporal_feature = create_temporal_features(meteorological_df, datetime_column_name)
    
    for mapper_cyclic_key in MAPPER_CYCLIC_VALUE:
        filtered_colum = [ tmp_el for tmp_el in created_temporal_feature if tmp_el.startswith(mapper_cyclic_key) ]
        if len(filtered_colum) > 0:
            print(f"Creando variables cíclica para {filtered_colum[0]} con el periodo de {MAPPER_CYCLIC_VALUE[mapper_cyclic_key]}")
            merged_df = create_cyclic_features(merged_df, filtered_colum[0], MAPPER_CYCLIC_VALUE[mapper_cyclic_key])

    print(create_cyclic_meteorological_features, merged_df)
    return merged_df, created_temporal_feature

def create_lag_meteorological_features(
        meteorological_df: pd.DataFrame,
        features: List[str],
        lag_values: List[int]
    ):

    for col in features:
        meteorological_df = create_lag_features(meteorological_df, col, lag_values)
    print("create_lag_meteorological_features", meteorological_df)
    return meteorological_df

def create_rolling_meteorological_features(
    meteorological_df: pd.DataFrame,
    features: pd.DataFrame,
    windows_rolling: List[int]
):
    for col in features:
        meteorological_df = create_rolling_features(meteorological_df, col, windows_rolling)

    print("create_rolling_meteorological_features", meteorological_df)

    return meteorological_df

def create_cinematic_feautes(
    meteorological_df: pd.DataFrame,
    velocity_column_name: str
):
    new_features = ['VELO_SQ', 'VELO_SQ']
    meteorological_df['VELO_SQ'] = meteorological_df[velocity_column_name]**2
    meteorological_df['VELO_INV'] = 1 / (meteorological_df[velocity_column_name] + 1e-6)

    print("create_cinematic_feautes", meteorological_df)

    return meteorological_df, new_features

def create_label_station_feature(
    meteorological_df: pd.DataFrame,
):  
    dummies = pd.get_dummies(meteorological_df, columns=['STATION_ID'], prefix='LABEL_STATION_ID')
    print("el dummy",dummies)
    return dummies

def get_meteorological_features(
    db_name: str, 
    table_name: str,
    features_names: List[str],
    start_period: str,
    end_period: str
) -> pd.DataFrame:
    
    db_client = FireStoreClient(db_name, table_name)
    elements = db_client.get_elements_by_filters(
        filters=[
            ('READING_DATETIME', '>=', start_period),
            ('READING_DATETIME', '<=', end_period),
        ],
        projections=[*features_names, 'READING_DATETIME']
        )
    df = pd.DataFrame(elements)

    return df.dropna()

def ingest_features(db_name: str, table_name: str, data: pd.DataFrame):
    db_client = FireStoreClient(db_name, table_name)
    print("New Data", data.head())
    print("Elements to add", data.shape)

    results = data.to_dict(orient='records')
    db_client.bulk_insert_elements(results)