"""
This is a boilerplate pipeline 'batch_process'
generated using Kedro 0.19.13
"""
import fitz
import pandas_gbq
from typing import List, Any, Dict, Union, Optional, Tuple
from google.cloud import storage
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from mlops_pm25.pipelines.batch_process.utils import generate_uuid, get_file_name
from mlops_pm25.pipelines.batch_process.repository.workflow_agent_client import WorkFlowAgent
from mlops_pm25.pipelines.batch_process.constants import ContentFileExtensionEnum, ContentMimeTypeEnum, DocumentTypeEnum
from mlops_pm25.pipelines.batch_process.constants import MAX_PARALLEL_WORKERS, AUNA_BLOB_DIRECTORY, DOCUMENT_WINDOW_SIZE, DEFUALT_ENTITIES
from mlops_pm25.pipelines.commons.domain.schemas import MAPPER_AUNA_DOCS
from mlops_pm25.pipelines.commons.domain.constants import BigQueryInsertionMode
from .utils import normalize_columns, print_ocr_agent_welcome, print_ocr_agent_good_bye, get_document_info
from mlops_pm25.pipelines.batch_process.repository.retry_manager import retry_manager

def get_pdfs_from_bucket(bucket: str, directory_path: str) -> List[Dict[str,str]]:
    print_ocr_agent_welcome()
    cliente_storage = storage.Client()
    bucket = cliente_storage.bucket(bucket)
    files_list = [ 
                    {
                        "id": generate_uuid(),
                        "file_path": blob.name,
                        **get_document_info(blob.name)
                    } 
                    for blob in bucket.list_blobs(prefix=directory_path)
                    if blob.name.endswith(ContentFileExtensionEnum.PDF.value) 
                ]
    return files_list

def divide_document(bucket_name: str, document_info: Dict[str, str]) -> Dict[str, Union[str, List[str]]]:

    process_id = document_info["id"]
    file_path = document_info["file_path"]

    client = storage.Client()
    source_blob_name = file_path
    try:
        bucket = client.get_bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        pdf_bytes = blob.download_as_bytes()

        pdf = fitz.open("pdf", pdf_bytes)
        output_paths = []
        
        for i in range(len(pdf)):
            try:
                single_page_pdf = fitz.open()
                single_page_pdf.insert_pdf(pdf, from_page=i, to_page=i)
                pdf_bytes = single_page_pdf.tobytes()
                single_page_pdf.close()
                
                output_page_blob_name = f"tmp/{AUNA_BLOB_DIRECTORY}/{get_file_name(file_path)}/page_{i+1}{ContentFileExtensionEnum.PDF.value}"
                output_blob = bucket.blob(output_page_blob_name)
                output_blob.upload_from_string(pdf_bytes, content_type=ContentMimeTypeEnum.PDF.value)
                output_paths.append(f"gs://{bucket_name}/{output_page_blob_name}")

            except Exception as error:
                print(f"There is a problem {error} in {file_path} in this page {i+1}")
                continue
        return {
            "id": process_id,
            "pages_path": output_paths
        }

    except Exception as error:
        print(f"There is a global problem {error} in {file_path}")
        return {
            "id": process_id,
            "pages_path": []
        }

def parallel_process_pdfs(
        bucket: str, pdf_paths: List[Dict[str, str]], 
        parallel_workers: Optional[int] = MAX_PARALLEL_WORKERS) -> List[Dict[str, Union[str, List[str]]]]:
    
    with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
        results = list(executor.map(
            lambda path: divide_document(bucket, path),
            pdf_paths
        ))

    return results

def extract_entities(documents_info: Dict[str, Union[str, List[str]]], max_workers: int = MAX_PARALLEL_WORKERS) ->  Tuple[List[Dict[str, Any]], List[Any]]:
    
    WorkFlowAgent.initialize_pool(pool_size=max_workers)
    error_executions = []
    unique_list_entities = []

    try:
        all_pages = [
            (page, doc["id"], doc["pages_path"][index+1:(index+1+DOCUMENT_WINDOW_SIZE)])
            for doc in documents_info
            for index, page in enumerate(doc["pages_path"])
        ]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    lambda args: WorkFlowAgent(file=args[0], process_id=args[1],
                                                additional_files=args[2]).execute_task(),
                    page_data
                ): page_data[1]
                for page_data in all_pages
            }
            
            for future in as_completed(futures):
                unique_list_entities.append(future.result())
            print("response entities", unique_list_entities)

            filtered_unique_list_entities = filter(lambda x: x is not None, unique_list_entities)
            unique_list_entities = list(filtered_unique_list_entities)
    finally:
        WorkFlowAgent.close_pool()
        error_executions = retry_manager.get_failed_items()
    
    return unique_list_entities, error_executions

def group_by_documents(recognized_entities: List[Dict[str,Any]]) -> Dict[str, Any]:
    
    grouped_entities = {}
    print("recognized_entities", recognized_entities)
    for entitie in recognized_entities:
        
        if entitie["document_type"] not in grouped_entities.keys():
            grouped_entities[entitie["document_type"]] = []

        grouped_entities[entitie["document_type"]].append(
            {
                **entitie["entities"], 
                "documento_id": entitie["process_id"],
                "page_path": entitie["file_path"]
            } if isinstance(entitie["entities"], dict) else {}
        )    

    return grouped_entities.get(DocumentTypeEnum.INVOICE.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.SITED.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.CREDIT_NOTE.value, DEFUALT_ENTITIES),\
            grouped_entities.get(DocumentTypeEnum.PRE_SETTLEMENT.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.SETTLEMENT_SUMMARY.value, DEFUALT_ENTITIES),\
            grouped_entities.get(DocumentTypeEnum.SETTLEMENT_TYPE_1.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.SETTLEMENT_TYPE_2.value, DEFUALT_ENTITIES), \
            grouped_entities.get(DocumentTypeEnum.GUARANTEE_LETTER.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.PRESCRIPTION.value, DEFUALT_ENTITIES), \
            grouped_entities.get(DocumentTypeEnum.EPICRISIS.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.PHARMACY_ATTENTION.value, DEFUALT_ENTITIES), grouped_entities.get(DocumentTypeEnum.OPERATORY_REPORT.value, DEFUALT_ENTITIES)

def save_df(project_id: str, 
            dataset: str,
            table_name: str,
            dataframe: pd.DataFrame,
            how_to_save: Optional[BigQueryInsertionMode]=BigQueryInsertionMode.APPEND.value
            ) -> bool:
    
    if dataframe.empty:
        return False

    print(dataframe.info())
    print(f"Dataframe size {dataframe.shape}")
    pandas_gbq.to_gbq(dataframe, f'{dataset}.{table_name}', project_id=project_id, 
                      if_exists=how_to_save)
    return True

def good_bye_message(invoice_df: pd.DataFrame, sited_df: pd.DataFrame, credit_note_df: pd.DataFrame):
    print_ocr_agent_good_bye()

def reprocess_failed_executions():
    return 
