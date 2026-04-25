from google.cloud import bigquery
import pandas as pd

class BigQueryClient:
    def __init__(self, project_id, dataset, table_name):
        
        self.client = bigquery.Client(project=project_id)
        self.dataset_ref = self.client.dataset(dataset)
        self.table_ref = self.dataset_ref.table(table_name)

    def streaming_dataframe_load(self, df: pd.DataFrame) -> None:    

        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
            autodetect=False,
            schema_update_options=[
                bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION
            ],
            time_partitioning=bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field="timestamp_prediccion"
            ),
            clustering_fields=["modelo", "cliente_id"]
        )
        
        job = self.client.load_table_from_dataframe(
            df, self.table_ref, job_config=job_config
        )
        job.result()
