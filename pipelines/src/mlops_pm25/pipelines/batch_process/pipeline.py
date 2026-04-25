"""
This is a boilerplate pipeline 'batch_process'
generated using Kedro 0.19.13
"""

from kedro.pipeline import node, Pipeline, pipeline  # noqa
from .nodes import get_pdfs_from_bucket, parallel_process_pdfs, extract_entities
from .nodes import group_by_documents
from .nodes import save_df, good_bye_message
from mlops_pm25.pipelines.commons.nodes import convert_json_to_df

def create_pipeline(**kwargs) -> Pipeline:
    return pipeline([
        node(
            func= get_pdfs_from_bucket,
            inputs= ["params:bucket", "params:directory_path"] ,
            outputs="file_list",
            name="Get_Pdf_Documents_From_Bucket"
        ),
        node(
            func= convert_json_to_df,
            inputs= "file_list",
            outputs="file_df",
            name="Convert_Documents_Json_2_Df"
        ),
        node(
            func= save_df,
            inputs= ["params:project_id","params:auna_process_dataset",
                     "params:document_table", "file_df", "params:insertion_mode"],
            outputs=None,
            name="Save_Document_Entities"
        ),
        node(
            func= parallel_process_pdfs,
            inputs= ["params:bucket", "file_list", "params:division_workers"] ,
            outputs="divided_file_list",
            name="Divide_by_Page_every_Pdf_Documents",
            tags=["parallel"]
        ),
        node(
            func= extract_entities,
            inputs= ["divided_file_list", "params:document_processing_workers"] ,
            outputs=["entitie_list", "failed_executions"],
            name="Get_Entities_From_Pdf_Documents",
            tags=["parallel"]
        ),
        node(
            func= group_by_documents,
            inputs= "entitie_list",
            outputs=[
                        "invoice_entities", "sited_entities", "credit_note_entities",
                        "pre_settlement_entities", "settlement_summary_entities",
                        "settlement_type_1_entities", "settlement_type_2_entities",
                        "guarantee_letter_entities", "prescription_entities", "epicrisis_entities",
                        "pharmacy_attention_entities", "operatory_report_entities"
                     ],
            name="Group_By_Document_Type"
        ),
        node(
            func= convert_json_to_df,
            inputs= "failed_executions",
            outputs="failed_executions_df",
            name="Convert_Failed_Executions_Json_2_Df"
        ),
        node(
            func= save_df,
            inputs= ["params:project_id","params:auna_process_dataset",
                     "params:failed_process_table", "failed_executions_df", "params:insertion_mode"],
            outputs=None,
            name="Save_Failed_Executions"
        ),
        node(
            func= good_bye_message,
            inputs= ["invoice_entities", "sited_entities", "credit_note_entities"],
            outputs=None,
            name="Process_Good_Bye_Message"
        )
    ])
