"""
This is a boilerplate pipeline 'export_to_bigquery'
generated using Kedro 0.19.13
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import look_up_extracted_entity, get_entity_table_information
from mlops_pm25.pipelines.commons.nodes import convert_json_to_df, save_prediction

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= look_up_extracted_entity,
            inputs= ["params:entity_name", "catalog"],
            outputs="selected_document_entity",
            name="Look_Up_Extracted_Entity"
        ),
        node(
            func= get_entity_table_information,
            inputs= ["params:entity_name", "params:entity_table_information"],
            outputs=["entity_table", "entity_schema"],
            name="Get_Entity_Information"
        ),
        node(
            func= convert_json_to_df,
            inputs= "selected_document_entity",
            outputs="selected_document_entity_df",
            name="Convert_Entity_Json_2_Df"
        ),
        node(
            func= save_prediction,
            inputs= ["params:project_id","params:auna_process_dataset",
                     "entity_table", "selected_document_entity_df", "entity_schema", "params:insertion_mode"],
            outputs="saved_entity",
            name="Save_Entity"
        )
    ])
