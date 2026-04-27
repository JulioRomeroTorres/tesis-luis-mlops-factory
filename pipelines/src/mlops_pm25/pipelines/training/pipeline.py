"""
This is a boilerplate pipeline 'training'
generated using Kedro 0.19.15
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import (
    get_meteorological_features, merge_information, 
    create_cyclic_meteorological_features,
    create_lag_meteorological_features,
    create_rolling_meteorological_features,
    create_cinematic_feautes,
    training, save_artifacts
)

def create_pipeline(**kwargs) -> Pipeline:

    return pipeline([
        node(
            func= get_meteorological_features,
            inputs= [
                    "params:db_name",
                    "params:feature_table_name", "params:features_names",
                     "params:start_period",
                     "params:end_period"],
            outputs="feature_data",
            name="Get_Feature_Data"
        ),
        node(
            func= get_meteorological_features,
            inputs= [
                    "params:db_name",
                    "params:target_table_name", "params:target_names",
                     "params:start_period",
                     "params:end_period"],
            outputs="target_data",
            name="Get_Targe_Data"
        ),
        node(
            func= merge_information,
            inputs= ["feature_data", "target_data", "params:datetime_column_name",],
            outputs="merged_data",
            name="Merged_Data"
        ),

        node(
            func= create_cyclic_meteorological_features,
            inputs= ["merged_data", "params:datetime_column_name"],
            outputs="cyclic_meteorological_data",
            name="Get_Cyclic_Data"
        ),

        node(
            func= create_lag_meteorological_features,
            inputs= ["cyclic_meteorological_data", "params:features_names",
                     "params:lag_features"],
            outputs="lag_meteorological_feature_data",
            name="Get_Lag_Meteorological_Feature_Data"
        ),

        node(
            func= create_lag_meteorological_features,
            inputs= ["lag_meteorological_feature_data", "params:target_names",
                     "params:lag_target"],
            outputs="lag_meteorological_target_data",
            name="Get_Lag_Meteorological_Target_Data"
        ),

        node(
            func= create_rolling_meteorological_features,
            inputs= ["lag_meteorological_target_data", "params:features_names",
                     "params:windows_rolling"],
            outputs="rolling_meteorological_feature_data",
            name="Get_Rolling_Meteorological_Feature_Data"
        ),

        node(
            func= create_rolling_meteorological_features,
            inputs= ["lag_meteorological_target_data", "params:target_names",
                     "params:windows_rolling"],
            outputs="rolling_meteorological_target_data",
            name="Get_Rolling_Meteorological_Target_Data"
        ),
        
        node(
            func= create_cinematic_feautes,
            inputs= ["rolling_meteorological_target_data", "params:velocity_column_name"],
            outputs=["cinematic_data", "cinematic_features"],
            name="Get_Cinematic_Data"
        ),

        node(
            func= training,
            inputs= ["cinematic_data", "params:target_names", "params:hyperparameters"],
            outputs=["model", "rmse_scores", "mae_scores", "r2_scores", "feature_importances"],
            name="Training_model"

        ),
        node(
            func= save_artifacts,
            inputs= [ "params:bucket_name", "params:blob_directory", "params: version",
                "model", "rmse_scores", "mae_scores", "r2_scores", "feature_importances"],
            outputs=None,
            name="Save_Artifacts"
        ),

    ])
