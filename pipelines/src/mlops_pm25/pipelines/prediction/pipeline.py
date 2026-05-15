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
    get_features, get_pm25, create_template_datetime_df, create_source_dataset,
    predict, save_predictions
) 

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([

        node(
            func= get_features,
            inputs= ["merged_data"],
            outputs="converted_type_df",
            name="ConvertedType"
        ),

        node(
            func= get_pm25,
            inputs= ["merged_data"],
            outputs="converted_type_df",
            name="ConvertedType"
        ),

        node(
            func= create_template_datetime_df,
            inputs= ["merged_data"],
            outputs="converted_type_df",
            name="ConvertedType"
        ),

        node(
            func= create_source_dataset,
            inputs= ["merged_data"],
            outputs="converted_type_df",
            name="ConvertedType"
        ),

        node(
            func= convert_type_columns,
            inputs= ["merged_data"],
            outputs="converted_type_df",
            name="ConvertedType"
        ),

        node(
            func= create_cyclic_meteorological_features,
            inputs= ["converted_type_df", "params:datetime_column_name"],
            outputs=["cyclic_meteorological_data", "cyclic_features"],
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
            inputs= ["lag_meteorological_feature_data", "params:targets_names",
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
            inputs= ["rolling_meteorological_feature_data", "params:targets_names",
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
            func= create_label_station_feature,
            inputs= ["cinematic_data"],
            outputs="label_station_id_df",
            name="Get_Label_Sttion_Data"
        ),

        node(
            func= predict,
            inputs= [
                    "label_station_id_df", "params:targets_names",
                    "params:bucket_name", "params:blob_directory", "params:version"
                     ],
            outputs="label_station_id_df",
            name="Get_Label_Sttion_Data"
        )


    ])
