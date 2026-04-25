import re 
ja = "20102756364_01_F135_00022092.pdf"

def regex_invoice_number(invoice_info: str)-> str:
    match = re.search(r'(\d+)_(\d+)_([A-Z]\d+)_(\d+)\.pdf', invoice_info)

    if match:
        return f"{match.group(3)}-{match.group(4)}"
    return ""

print(regex_invoice_number(ja))

from google.cloud import storage
import os

def download_file_from_bucket(bucket_name, source_blob_name, destination_file_path):
    """
    Descarga un archivo desde Google Cloud Storage
    
    Args:
        bucket_name (str): Nombre del bucket GCS (ej: 'mi-bucket')
        source_blob_name (str): Ruta del archivo en el bucket (ej: 'carpeta/archivo.txt')
        destination_file_path (str): Ruta local donde guardar el archivo (ej: './descargas/archivo.txt')
    """
    # Inicializa el cliente de Storage
    storage_client = storage.Client()
    
    try:
        # Obtiene referencia al bucket y blob
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        
        # Descarga el archivo
        blob.download_to_filename(destination_file_path)
        
        print(f"Archivo {source_blob_name} descargado como {destination_file_path}")
        return True
    
    except Exception as e:
        print(f"Error al descargar el archivo: {e}")
        return False

# Ejemplo de uso
if __name__ == "__main__":
    # Configuración

    BUCKET_NAME = "us-east4-rs-nprd-dlk-ia-dev-9f187df9-bucket"
    SOURCE_BLOB_NAME = "dags/kedro_pipeline_genai_auna_batch_process.py"  # Ruta dentro del bucket
    DESTINATION_FILE = "./kedro_pipeline_genai_auna_batch_process.py"  # Ruta local
    
    # Asegura que el directorio de destino exista
    os.makedirs(os.path.dirname(DESTINATION_FILE), exist_ok=True)
    
    # Descarga el archivo
    download_file_from_bucket(BUCKET_NAME, SOURCE_BLOB_NAME, DESTINATION_FILE)