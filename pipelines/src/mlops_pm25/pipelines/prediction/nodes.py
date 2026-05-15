"""
This is a boilerplate pipeline 'prediction'
generated using Kedro 0.19.15
"""
import pandas as pd
from typing import List
from datetime import timedelta
from kedro_datasets.pickle import PickleDataset
from mlops_pm25.pipelines.commons.repository.firestore_client import FireStoreClient
from mlops_pm25.pipelines.commons.utils import is_str_none, get_current_datetime

def get_last_evaluation_date(start_period: str, end_period: str):
    return is_str_none(start_period) and is_str_none(end_period)

def get_features(db_name: str, table_name: str, start_period: str, end_period: str):
    db_client = FireStoreClient(db_name, table_name)
    print("New Data", data.head())
    print("Elements to add", data.shape)

    results = data.to_dict(orient='records')
    db_client.bulk_insert_elements(results)

def get_pm25(db_name: str, table_name: str, start_period: str, end_period: str):
    pass

def create_template_datetime_df(start_period: str, end_period: str, station_ids: List[str]) -> pd.DataFrame:
    lastest_datetime = get_last_evaluation_date(start_period, end_period)

    if lastest_datetime:
        current_datetime = (get_current_datetime()-timedelta(hours=5))
        lower_limit_datetime = (get_current_datetime()-timedelta(hours=41))

        completed_datetime_list = []
        default_station_id_list = []

        while( lower_limit_datetime <= current_datetime):
            completed_datetime_list.append(
                lower_limit_datetime.strftime("%d/%m/%Y%H:00:")
            )
            lower_limit_datetime = lower_limit_datetime + timedelta(hours=1)

        total_hours_list = len(completed_datetime_list)
        
        for station_id in station_ids:
            station_id_list= [station_id]*total_hours_list
            default_station_id_list = [*station_id_list, *default_station_id_list] 

        updated_list = completed_datetime_list*len(station_ids)

        
        df_datetimes = pd.DataFrame({
                                        "COMPLETED_READING_DATETIME": updated_list,
                                        "SOURCE_STATION_ID": default_station_id_list
                                        })  

        return  df_datetimes

def create_source_dataset(template_datetime_df: pd.DataFrame, features_df: pd.DataFrame, pm25_df: pd.DataFrame ):

    features_merged_df = template_datetime_df.merge(
        features_df,
        left_on=['COMPLETED_READING_DATETIME', 'SOURCE_STATION_ID'],
        right_on=['READING_DATETIME', 'STATION_ID'],
        how='left'
    )

    completed_merged_df = features_merged_df.merge(
        pm25_df,
        left_on=['READING_DATETIME', 'STATION_ID'],
        right_on=['READING_DATETIME', 'STATION_ID'],
        how='left'
    )

    completed_merged_df = completed_merged_df[['READING_DATETIME', 'STATION_ID', 'VELO', 'HUME', 'TEMP', 'N_PM25']]
    filled_df = completed_merged_df.fillna(completed_merged_df.mean(numeric_only=True))

    return filled_df


def predict(
    meteorological_df: pd.DataFrame,
    target_columns: List[str],
    bucket_name: str, blob_directory: str, version: str,
    start_period: str, end_period: str
):
    lastest_datetime = get_last_evaluation_date(start_period, end_period)
    base_path = f"gs://{bucket_name}/{blob_directory}/{version}"
    model_artifact_path = f"{base_path}/model.joblib"

    features_columns = [col for col in meteorological_df.columns if ( col not in target_columns) and not ( col.startswith('READING_DATETIME') or col.startswith('READING_DATETIME_') or col.startswith('STATION_ID'))  ]
    X = meteorological_df[features_columns]

    dataset = PickleDataset(
        filepath=model_artifact_path,
        backend="joblib"
    )
    
    model = dataset.load()
    y_pred = model.predict(X)

    print("Predictions", y_pred)

    #df_unido = pd.concat([df_izquierdo, df_derecho], axis=1)

    #if lastest_datetime:
    #    return []
    
    #return 


def save_predictions():
    pass