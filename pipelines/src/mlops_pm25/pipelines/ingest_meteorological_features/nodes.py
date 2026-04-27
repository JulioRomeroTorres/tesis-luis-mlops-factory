"""
This is a boilerplate pipeline 'ingest_meteorical_features'
generated using Kedro 0.19.15
"""
import pandas as pd
from typing import List, Dict, Any
from mlops_pm25.pipelines.commons.repository.senamhi_transformer import SenamhiTransformer
from mlops_pm25.pipelines.commons.repository.firestore_client import FireStoreClient

def get_meteorological_variables_data(
        station_id:str, var_names:List[str], 
        endpoint: str, start_period: str,
        end_period: str,
        ):
    all_predicted_data = {}
    for var_name in var_names:
        senamhi_transformer = SenamhiTransformer(endpoint)
        var_time_line, variable_values = senamhi_transformer.get_variable_data(station_id, var_name, start_period, end_period)

        all_predicted_data[f'READING_DATETIME_{var_name}'] = var_time_line
        all_predicted_data[var_name] = variable_values
        all_predicted_data['STATION_ID'] = [station_id]*len(variable_values)

    all_predicted_data['READING_DATETIME'] = all_predicted_data[f'READING_DATETIME_{var_names[0]}']
    print("Hey Data", all_predicted_data)
    df = pd.DataFrame(all_predicted_data)

    return df[['READING_DATETIME', *var_names, 'STATION_ID']]

def get_meteorological_data_by_station(
    stations_ids: List[str], var_names: List[str], 
    endpoint: str, start_period: str,
    end_period: str,
):

    meteorological_df = pd.DataFrame()
    lista_dfs = []

    for station_id in stations_ids:
        station_meteorological_df = get_meteorological_variables_data(station_id, var_names, endpoint, start_period, end_period)

        if not station_meteorological_df.empty:
            print(station_meteorological_df.head())
            lista_dfs.append(station_meteorological_df)

    meteorological_df = pd.concat(lista_dfs, ignore_index=True)
    return meteorological_df

def ingest_features(db_name: str, table_name: str, data: pd.DataFrame):
    db_client = FireStoreClient(db_name, table_name)

    results = data.to_dict(orient='records')
    db_client.bulk_insert_elements(results)
