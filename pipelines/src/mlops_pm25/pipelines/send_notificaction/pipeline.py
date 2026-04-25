"""
This is a boilerplate pipeline 'send_notificaction'
generated using Kedro 0.19.14
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import send_notification, generate_excel_df_query, generate_consolidated_df_query, generate_consolidated_total_rejected_df_query
from mlops_pm25.pipelines.commons.nodes import load_source_data
from .nodes import save_files, valid_dataframe_dimension

def create_pipeline(**kwargs) -> Pipeline:

    return pipeline([
        node(
            func= generate_excel_df_query,
            inputs= ["params:project_id"],
            outputs="generate_excel_df_query",
            name="Create_excel_data_query"
        ),
        node(
            func= load_source_data,
            inputs= ["generate_excel_df_query"],
            outputs="excel_process_data",
            name="excel_data_query"
        ),

        node(
            func= generate_consolidated_total_rejected_df_query,
            inputs= ["params:project_id"],
            outputs="generate_consolidated_total_rejected_df_query",
            name="Consolidated_total_Rejected_Data_Query"
        ),
        node(
            func= load_source_data,
            inputs= ["generate_consolidated_total_rejected_df_query"],
            outputs="summary_total_rejected_process_data",
            name="Consolidated_total_Rejected_Data"
        ),

        node(
            func= generate_consolidated_df_query,
            inputs= ["params:project_id"],
            outputs="generate_consolidated_df_query",
            name="Create_Consolidated_data_query"
        ),
        node(
            func= load_source_data,
            inputs= ["generate_consolidated_df_query"],
            outputs="summary_process_data",
            name="Consolidated_data"
        ),

        node(
            func= save_files,
            inputs= ["excel_process_data", "params:process_period", "params:pipeline_bucket", "params:common_file_path"],
            outputs= "path_xlsx_file",
            name="Save_Files"
        ),

        node(
            func= valid_dataframe_dimension,
            inputs= "excel_process_data",
            outputs= "valid_dimension_flag",
            name="Valid_Xlsx_Dataframe_Dimension"
        ),

        node(
            func= send_notification,
            inputs= [
                "params:from_email", "params:destinatiries_list", "params:cc_list",
                "params:pipeline_bucket", "path_xlsx_file", "summary_process_data", "summary_total_rejected_process_data" , "params:process_period","valid_dimension_flag"],
            outputs=None,
            name="Send_notification"
        )
    ])
