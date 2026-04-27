"""
This is a boilerplate pipeline 'training'
generated using Kedro 0.19.15
"""
import numpy as np
import xgboost as xgb
import pandas as pd
from typing import List, Dict, Any, Optional

from .models import XgboostHyperParameters
from .domain.constants import MAPPER_CYCLIC_VALUE

from sklearn.model_selection import TimeSeriesSplit
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

from mlops_pm25.pipelines.commons.repository.firestore_client import FireStoreClient
from .domain.utils import (
    create_lag_features, create_rolling_features,
    create_cyclic_features, create_temporal_features
) 

from kedro_datasets.pickle import PickleDataset

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
    
    return pd.DataFrame(elements)

def merge_information(
    first_df: pd.DataFrame,
    second_df: pd.DataFrame,
    inner_column: str
):
    return pd.merge(
        first_df,
        second_df,
        left_on=inner_column,
        right_on=inner_column,
        how='inner'
    )

def create_cyclic_meteorological_features(meteorological_df: pd.DataFrame, datetime_column_name: str):
    merged_df, created_temporal_feature = create_temporal_features(meteorological_df, datetime_column_name)
    
    for mapper_cyclic_key in MAPPER_CYCLIC_VALUE:
        filtered_colum = [ tmp_el for tmp_el in created_temporal_feature if tmp_el.startswith(mapper_cyclic_key) ]
        if len(filtered_colum) > 0:
            print(f"Creando variables cíclica para {filtered_colum[0]} con el periodo de {MAPPER_CYCLIC_VALUE[mapper_cyclic_key]}")
            merged_df = create_cyclic_features(merged_df, filtered_colum[0], MAPPER_CYCLIC_VALUE[mapper_cyclic_key])

    return merged_df, created_temporal_feature

def create_lag_meteorological_features(
        meteorological_df: pd.DataFrame,
        features: List[str],
        lag_values: List[int]
    ):

    for col in features:
        meteorological_df = create_lag_features(meteorological_df, col, lag_values)
    return meteorological_df

def create_rolling_meteorological_features(
    meteorological_df: pd.DataFrame,
    features: pd.DataFrame,
    windows_rolling: List[int]
):
    for col in features:
        meteorological_df = create_rolling_features(meteorological_df, col, windows_rolling)

    return meteorological_df

def create_cinematic_feautes(
    meteorological_df: pd.DataFrame,
    velocity_column_name: str
):
    new_features = ['VELO_SQ', 'VELO_SQ']
    meteorological_df['VELO_SQ'] = meteorological_df[velocity_column_name]**2
    meteorological_df['VELO_INV'] = 1 / (meteorological_df[velocity_column_name] + 1e-6)

    return meteorological_df, new_features

def create_label_station_feature(
    meteorological_df: pd.DataFrame,
):
    return pd.get_dummies(meteorological_df, columns=['STATION_ID'], prefix='LABEL_STATION_ID')

def training(
    meteorological_df: pd.DataFrame,
    target_columns: List[str],
    hyperparameters: Dict[str, Any] = {},
    features_columns: Optional[List[str]] = None,
    validation_split_ratio: float = 0.2,
    n_splits: int = 10,
    number_relevant_features: int = 20
):
    meteorological_df = meteorological_df.dropna()
    hyperparameters = XgboostHyperParameters(**hyperparameters)

    if features_columns is None:
        features_columns = [col for col in meteorological_df.columns if ( col not in target_columns) and not ( col.startswith('READING_DATETIME') or col.startswith('READING_DATETIME_') or col.startswith('STATION_ID'))  ]

    X = meteorological_df[features_columns]
    y = meteorological_df[target_columns]

    tscv = TimeSeriesSplit(n_splits=n_splits)

    print(f"\nUsando TimeSeriesSplit con {n_splits} splits para validación cruzada.")

    rmse_scores = []
    mae_scores = []
    r2_scores = []

    for fold, (train_index, test_index) in enumerate(tscv.split(X)):
        print(f"--- Fold {fold+1}/{n_splits} ---")

        X_train, X_test = X.iloc[train_index], X.iloc[test_index]
        y_train, y_test = y.iloc[train_index], y.iloc[test_index]

        val_idx = int(len(X_train) * (1 - validation_split_ratio))

        X_train_split, X_val = X_train.iloc[:val_idx], X_train.iloc[val_idx:]
        y_train_split, y_val = y_train.iloc[:val_idx], y_train.iloc[val_idx:]

        print(f"  Tamaño X Train (fold): {X_train_split.shape[0]}, Val (fold): {X_val.shape[0]}, Test (fold): {X_test.shape[0]}")
        print(f"  Tamaño y Train (fold): {y_train_split.shape[0]}, Val (fold): {y_val.shape[0]}, Test (fold): {y_test.shape[0]}")

        model = xgb.XGBRegressor(
            objective='reg:squarederror',
            n_estimators= hyperparameters.n_estimators,
            learning_rate=hyperparameters.learning_rate,
            max_depth=hyperparameters.max_depth,
            subsample=hyperparameters.subsample,
            colsample_bytree=hyperparameters.colsample_bytree,
            random_state=hyperparameters.random_state,
            n_jobs=hyperparameters.n_jobs,
            gamma=hyperparameters.gamma,
            reg_alpha=hyperparameters.reg_alpha,
            reg_lambda=hyperparameters.reg_lambda
        )

        model.fit(X_train_split, y_train_split,
                eval_set=[(X_val, y_val)],
                verbose=False)

        y_pred = model.predict(X_test)

        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        mae = mean_absolute_error(y_test, y_pred)
        r2 = r2_score(y_test, y_pred)

        print(f"  Resultados Fold {fold+1}: RMSE={rmse:.2f}, MAE={mae:.2f}, R²={r2:.2f}")

        rmse_scores.append(rmse)
        mae_scores.append(mae)
        r2_scores.append(r2)

    # --- 5. Evaluación General (Promedio de los Folds) ---

    print("\n--- Resultados Generales (Promedio de Folds) ---")
    mean_rmse = np.mean(rmse_scores)
    mean_mae = np.mean(mae_scores)
    mean_r2 = np.mean(r2_scores)

    print(f"Promedio RMSE: {mean_rmse:.2f}")
    print(f"Promedio MAE: {mean_mae:.2f}")
    print(f"Promedio R²: {mean_r2:.2f}")

    print("\nImportancia de las características (último fold):")
    feature_importances = pd.DataFrame({
        'feature': features_columns,
        'importance': model.feature_importances_
    })

    feature_importances = feature_importances.sort_values('importance', ascending=False)
    feature_importances = feature_importances.head(number_relevant_features)

    return model, rmse_scores, mae_scores, r2_scores, feature_importances.to_dict(orient='records')

def save_artifacts(
    bucket_name: str, blob_directory: str,
    version: str, 
    model: Any, rmse_scores: List[float], mae_scores: List[float], r2_scores: List[float], feature_importances: List[str]
):

    base_path = f"gs://{bucket_name}/{blob_directory}/{version}"
    print(f"✅ Artefactor guardados directamente en: {base_path}")

    dataset = PickleDataset(
        filepath=f"{base_path}/model.joblib",
        backend="joblib",
        save_args={"compress": 3}
    )
    
    dataset.save(model)
    
    dataset = PickleDataset(
        filepath=f"{base_path}/artifacts.pickle",
        backend="pickle"
    )

    dataset.save({
        "rmse_scores": rmse_scores, 
        "mae_scores": mae_scores, 
        "r2_scores": r2_scores, 
        "feature_importances": feature_importances
    })

    return base_path