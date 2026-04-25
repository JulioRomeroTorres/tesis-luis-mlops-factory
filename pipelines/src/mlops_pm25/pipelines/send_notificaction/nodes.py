"""
This is a boilerplate pipeline 'send_notificaction'
generated using Kedro 0.19.14
"""
import pathlib
import pandas as pd
from typing import List, Optional
from datetime import timedelta
from mlops_pm25.pipelines.commons.repository.gcs_client import GcsClient
from mlops_pm25.pipelines.commons.repository.email_client import EmailClient
from mlops_pm25.pipelines.commons.utils import current_datetime, replace_variables, generate_query, is_empty_dataframe, scale_number, string_date_to_datetime
from mlops_pm25.pipelines.commons.domain.constants import ScaleNumber
from .constants import DEFAULT_ATTACHMENT_WITH_TOTAL_REJECTIONS_INFORMATION_TEMPLATE, AUNA_PROCESS_SUMMARY_SUBJECT 

def _get_template_query_from_file(source_directory: str, file_path: str, **kwargs) -> str:
    queries_folder = pathlib.Path(__file__).parent / source_directory
    query = generate_query(
                        f"{queries_folder}/{file_path}",
                        **kwargs
                    )
    return query

def generate_excel_df_query( 
                        project_id: str
                        ) -> str:
    queries_folder = pathlib.Path(__file__).parent / "queries"
    query = generate_query(
                        f"{queries_folder}/summary/get_alerts.sql",
                        project_id= project_id
                    )
    return query

def generate_consolidated_df_query( 
                        project_id: str
                        ) -> str:
    queries_folder = pathlib.Path(__file__).parent / "queries"
    query = generate_query(
                        f"{queries_folder}/summary/get_summary.sql",
                        project_id= project_id
                    )
    return query

def generate_consolidated_total_rejected_df_query(
                                        project_id: str
                                    ) -> str:
    queries_folder = pathlib.Path(__file__).parent / "queries"
    query = generate_query(
                        f"{queries_folder}/summary/get_total_rejection_summary.sql",
                        project_id= project_id
                    )
    return query

def valid_dataframe_dimension(
    source_df: pd.DataFrame
):
    return is_empty_dataframe(source_df)

def send_notification(
    from_email: str,
    destinatiries: List[str],
    list_cc: Optional[List[str]],
    bucket: str,
    xlsx_file_path: str,
    enrich_data: pd.DataFrame,
    enrich_total_rejected_data: pd.DataFrame,
    process_date: str,
    is_empty_xlsx_df: bool = False 
):
    
    if(is_empty_xlsx_df):
        print("Dataframe is empty")
        return

    signed_xlsx_url =  GcsClient(bucket).generate_signed_url(xlsx_file_path, 60*24*7)

    enrich_data = enrich_data.iloc[0].to_dict()
    enrich_total_rejected_data = enrich_total_rejected_data.iloc[0].to_dict()

    invoice_amount = enrich_data.get('mtoFacturado', 0)
    obs_amount = enrich_data.get('mtoObservado', 0)
    potencial_amount = enrich_data.get('mtoPotAhorro', 0)
    total_rejected_invoice_amount = enrich_total_rejected_data.get('mtoFacturado', 0)

    current_date = string_date_to_datetime(process_date)
    print(f"Current Datetime {current_date}")

    current_str_date = current_date.strftime('%d/%m')

    mapper_html_params = {
        '{signed_url}': signed_xlsx_url,
        '{current_date}': current_str_date,
        '{invoice_number}': f"{int(enrich_data.get('ctdFacturas', 0))}",
        '{invoice_amount}': scale_number(invoice_amount, ScaleNumber.MILLIONS.value),
        '{invoice_number_obs}': f"{int(enrich_data.get('ctdFactObservadas', 0))}", 
        '{invoice_number_obs_percentage}': f"{round(100*enrich_data.get('pctFactObs', 0), 2)}",
        '{obs_amount}': scale_number(obs_amount, ScaleNumber.MILLIONS.value),
        '{potencial_amount}': scale_number(potencial_amount, ScaleNumber.MILLIONS.value),
        '{total_rejected_invoice}':  f"{int(enrich_total_rejected_data.get('ctdFacturas', 0))}",
        '{total_rejected_amount}': scale_number(total_rejected_invoice_amount, ScaleNumber.MILLIONS.value),
    }

    html_body = replace_variables(DEFAULT_ATTACHMENT_WITH_TOTAL_REJECTIONS_INFORMATION_TEMPLATE, mapper_html_params)
    
    email_client = EmailClient(
        from_email,
        destinatiries,
        list_cc,
        html_body,
        AUNA_PROCESS_SUMMARY_SUBJECT
    )

    email_client.send_email()

def save_files(
    xlsx_dataframe: pd.DataFrame,
    period: str,
    bucket: str,
    directory_path: str
):
    common_file_path = f"{directory_path}/alertas_auna_{period}"
    
    bucket_path = f"gs://{bucket}"
    xlsx_path_file = f"{common_file_path}.xlsx"

    xlsx_dataframe.to_excel(f"{bucket_path}/{xlsx_path_file}", index=False)

    return xlsx_path_file