import numpy as np
import pandas as pd
from typing import Tuple, List


def create_temporal_features(df: pd.DataFrame, column_name: str) -> Tuple[pd.DataFrame, List[str]] :
    temporal_names = ['hour', 'dayofweek', 'month', 'dayofyear', 'year']
    created_features = [ f'{temporal_name}_{column_name}' for temporal_name in temporal_names]

    df[created_features[0]] = df[column_name].dt.hour
    df[created_features[1]] = df[column_name].dt.dayofweek
    df[created_features[2]] = df[column_name].dt.month
    df[created_features[3]] = df[column_name].dt.dayofyear
    df[created_features[4]] = df[column_name].dt.year
    return df, created_features

def create_cyclic_features(df: pd.DataFrame, col: str, period: int) -> pd.DataFrame:
    df[f'{col}_sin'] = np.sin(2 * np.pi * df[col] / period)
    df[f'{col}_cos'] = np.cos(2* np.pi * df[col] / period)
    return df

def create_lag_features(df: pd.DataFrame, column: str, lags: int)  -> pd.DataFrame:
    for lag in lags:
        df[f'{column}_lag_{lag}'] = df[column].shift(lag)
    return df

def create_rolling_features(df: pd.DataFrame, column: str, windows: int) -> pd.DataFrame:
    for window in windows:
        df[f'{column}_rolling_mean_{window}'] = df[column].rolling(window=window).mean()
        df[f'{column}_rolling_std_{window}'] = df[column].rolling(window=window).std()
    return df