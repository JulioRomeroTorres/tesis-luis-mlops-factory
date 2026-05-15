"""
This is a boilerplate pipeline 'prediction'
generated using Kedro 0.19.15
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from mlops_pm25.pipelines.commons.nodes import (
    convert_type_columns, create_cyclic_meteorological_features,
    create_rolling_meteorological_features, create_cinematic_feautes,
    create_lag_meteorological_features, create_label_station_feature
)
from .nodes import (
    get_features, create_template_datetime_df, create_source_dataset,
    predict, save_predictions
) 

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(
            func= get_features,
            inputs= [
                    "params:db_name",
                    "params:feature_table_name", "params:station_features_names",
                     "params:start_period",
                     "params:end_period"],
            outputs="pred_feature_data",
            name="Get_Pred_Feature_Data"
        ),

        node(
            func= get_features,
            inputs= [
                    "params:db_name",
                    "params:target_table_name", "params:station_targets_names",
                     "params:start_period",
                     "params:end_period"],
            outputs="pm25_target_data",
            name="Get_Target_PM25_Data"
        ),

        node(
            func= create_template_datetime_df,
            inputs= [
                "params:start_period",
                "params:end_period",
                "params:stations_ids"
            ],
            outputs="template_dataframe_df",
            name="Create_Template_Feature_Datetime"
        ),

        node(
            func= create_source_dataset,
            inputs= [
                "template_dataframe_df",
                "pred_feature_data",
                "pm25_target_data"
                ],
            outputs="source_pred_dataset",
            name="Source_Pred_Dataset"
        ),

        node(
            func= convert_type_columns,
            inputs= ["source_pred_dataset"],
            outputs="converted_type_pred_df",
            name="Converted_Prediction_Type"
        ),

        node(
            func= create_cyclic_meteorological_features,
            inputs= ["converted_type_pred_df", "params:datetime_column_name"],
            outputs=["cyclic_meteorological_pred_data", "cyclic_pred_features"],
            name="Get_Pred_Cyclic_Data"
        ),

        node(
            func= create_lag_meteorological_features,
            inputs= ["cyclic_meteorological_pred_data", "params:features_names",
                     "params:lag_features"],
            outputs="lag_meteorological_feature_pred_data",
            name="Get_Pred_Lag_Meteorological_Feature_Data"
        ),

        node(
            func= create_lag_meteorological_features,
            inputs= ["lag_meteorological_feature_pred_data", "params:targets_names",
                     "params:lag_target"],
            outputs="lag_meteorological_target_pred_data",
            name="Get_Lag_Meteorological_Target_Pred_Data"
        ),

        node(
            func= create_rolling_meteorological_features,
            inputs= ["lag_meteorological_target_pred_data", "params:features_names",
                     "params:windows_rolling"],
            outputs="rolling_meteorological_feature_pred_data",
            name="Get_Rolling_Meteorological_Feature_Pred_Data"
        ),

        node(
            func= create_rolling_meteorological_features,
            inputs= ["rolling_meteorological_feature_pred_data", "params:targets_names",
                     "params:windows_rolling"],
            outputs="rolling_meteorological_target_pred_data",
            name="Get_Rolling_Meteorological_Target_Pred_Data"
        ),
        
        node(
            func= create_cinematic_feautes,
            inputs= ["rolling_meteorological_target_pred_data", "params:velocity_column_name"],
            outputs=["cinematic_pred_data", "cinematic_pred_features"],
            name="Get_Cinematic_Pred_Data"
        ),

        node(
            func= create_label_station_feature,
            inputs= ["cinematic_pred_data"],
            outputs="label_station_id_pred_df",
            name="Get_Label_Sttion_Pred_Data"
        ),

        node(
            func= predict,
            inputs= [
                    "label_station_id_pred_df", "params:targets_names",
                    "params:bucket_name", "params:blob_directory", "params:version"
                     ],
            outputs=None,
            name="Predict_PM25"
        )


    ])
