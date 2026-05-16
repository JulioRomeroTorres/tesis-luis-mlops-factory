"""
This is a boilerplate pipeline 'prediction'
generated using Kedro 0.19.15
"""
import pandas as pd
from typing import List
from datetime import timedelta, datetime
from kedro_datasets.pickle import PickleDataset
from mlops_pm25.pipelines.commons.utils import is_str_none, get_current_datetime, recover_dummy_column
from mlops_pm25.pipelines.commons.nodes import get_meteorological_features, ingest_features

def get_last_evaluation_date(start_period: str, end_period: str):
    return is_str_none(start_period) and is_str_none(end_period)

def get_features(db_name: str, table_name: str, features_names: List[str], start_period: str, end_period: str):

    end_period = ( ( get_current_datetime()-timedelta(hours=5) ).strftime("%d/%m/%Y%H:00:")) if is_str_none(end_period) else end_period
    start_period = ( ( get_current_datetime()-timedelta(hours=41) ).strftime("%d/%m/%Y%H:00:"))  if is_str_none(start_period) else start_period
    
    return get_meteorological_features(
        db_name, 
        table_name,
        features_names,
        start_period,
        end_period
    )

def create_template_datetime_df(start_period: str, end_period: str, station_ids: List[str]) -> pd.DataFrame:

    end_period = ( ( get_current_datetime()-timedelta(hours=5) )) if is_str_none(end_period) else  datetime.strptime(end_period, '%d/%m/%Y%H:%M:') 
    start_period = ( ( get_current_datetime()-timedelta(hours=41)))  if is_str_none(start_period) else datetime.strptime(start_period, '%d/%m/%Y%H:%M:') 

    completed_datetime_list = []
    default_station_id_list = []

    while(start_period <= end_period):
        completed_datetime_list.append(
            start_period.strftime("%d/%m/%Y%H:00:")
        )
        start_period = start_period + timedelta(hours=1)

    total_hours_list = len(completed_datetime_list)
    
    for station_id in station_ids:
        station_id_list= [station_id]*total_hours_list
        default_station_id_list = [*station_id_list, *default_station_id_list] 

    updated_list = completed_datetime_list*len(station_ids)

    print(f"List station ids {station_ids}")
    
    df_datetimes = pd.DataFrame({
                                    "READING_DATETIME": updated_list,
                                    "STATION_ID": default_station_id_list
                                    })  
    print(df_datetimes.head())
    return  df_datetimes

def create_source_dataset(template_datetime_df: pd.DataFrame, features_df: pd.DataFrame, pm25_df: pd.DataFrame ):

    features_merged_df = template_datetime_df.merge(
        features_df,
        left_on=['READING_DATETIME', 'STATION_ID'],
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

    print("Los columns", meteorological_df.columns)
    
    dataset = PickleDataset(
        filepath=model_artifact_path,
        backend="joblib"
    )
    
    model = dataset.load()
    model_feature_names = model.get_booster().feature_names
    print("Feature Column Names", model_feature_names)
    
    X = meteorological_df[model_feature_names]
    
    y_pred = model.predict(X)
    print("Predictions", y_pred)

    information_station = meteorological_df[
        ['READING_DATETIME']
    ]

    information_station['STATION_ID'] = recover_dummy_column(meteorological_df[[ 'LABEL_STATION_ID_111286', 'LABEL_STATION_ID_111287',
       'LABEL_STATION_ID_112193', 'LABEL_STATION_ID_112233',
       'LABEL_STATION_ID_112265']])

    print("information_station", information_station)

    df_prediction = pd.DataFrame({'prediction': y_pred})

    concated_df = pd.concat([information_station, df_prediction], axis=1)

    if lastest_datetime:
        latest_datetime_value = ( (get_current_datetime()-timedelta(hours=5)).strftime("%d/%m/%Y%H:00:"))
        return concated_df[concated_df['READING_DATETIME'] == latest_datetime_value]
    return concated_df 


def save_predictions(db_name: str, table_name: str, prediction: pd.DataFrame):
    print("Save this predictions", prediction)
    ingest_features(db_name, table_name, prediction)
    pass