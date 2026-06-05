import asyncio
from datetime import datetime
from google.cloud.firestore import AsyncClient
from concurrent.futures import ThreadPoolExecutor
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Inicializar cliente asíncrono
db = AsyncClient(database='meteorological-data')

# Configuración
COLLECTION_NAME = 'senamhi-information'  # Cambia por el nombre de tu colección
BATCH_SIZE = 800
MAX_CONCURRENT_BATCHES = 8  # Número de batches simultáneos

def convert_to_timestamp(date_string):
    """
    Convertir string con formato fijo "DD/MM/YYYYHH:MM:" a Timestamp
    Ejemplo: "22/05/202519:00:" -> datetime(2025, 5, 22, 19, 0)
    """
    try:
        # Extraer por posición fija
        day = int(date_string[0:2])           # "22" -> 22
        month = int(date_string[3:5])         # "05" -> 5
        year = int(date_string[6:10])         # "2025" -> 2025
        hour = int(date_string[10:12])        # "19" -> 19
        minute = int(date_string[13:15])      # "00" -> 0
        
        # Crear datetime
        return datetime(year, month, day, hour, minute)
        
    except Exception as e:
        logger.error(f"Error convirtiendo '{date_string}': {e}")
        return None

async def get_all_document_ids():
    """Obtener todos los IDs de documentos de forma asíncrona"""
    doc_ids = []
    last_doc = None
    
    while True:
        # Construir query
        query = db.collection(COLLECTION_NAME).select(['READING_DATETIME', 'HUME', 'VELO', 'TEMP', 'STATION_ID']).limit(1500)
        
        if last_doc:
            query = query.start_after(last_doc)
        
        # Ejecutar query asíncrona
        docs = await query.get()
        
        if not docs:
            break
        
        doc_ids.extend([doc.id for doc in docs])
        last_doc = docs[-1]
        logger.info(f"Recopilados {len(doc_ids)} documentos...")
    
    logger.info(f"Total documentos encontrados: {len(doc_ids)}")
    return doc_ids

async def process_batch(doc_ids_batch):
    """Procesar un lote de documentos de forma asíncrona"""
    batch = db.batch()
    updated_count = 0
    
    for doc_id in doc_ids_batch:
        try:
            # Obtener documento asíncronamente
            doc_ref = db.collection(COLLECTION_NAME).document(doc_id)
            doc = await doc_ref.get()
            
            if not doc.exists:
                continue
            
            doc_data = doc.to_dict()
            reading_datetime = doc_data.get('READING_DATETIME')
            
            if reading_datetime and isinstance(reading_datetime, str):
                formatted_timestamp = convert_to_timestamp(reading_datetime)
                
                if formatted_timestamp:
                    batch.update(doc_ref, {
                        'FORMATED_READING_DATETIME': formatted_timestamp
                    })
                    updated_count += 1
                    
        except Exception as e:
            logger.error(f"Error en doc {doc_id}: {e}")
    
    # Ejecutar batch si hay actualizaciones
    if updated_count > 0:
        await batch.commit()
    
    return updated_count

async def add_formatted_datetime_parallel():
    """Procesar todos los documentos en paralelo con cliente asíncrono"""
    try:
        logger.info("Iniciando procesamiento paralelo asíncrono...")
        
        # Obtener todos los IDs
        all_doc_ids = await get_all_document_ids()
        
        if not all_doc_ids:
            logger.info("No se encontraron documentos")
            return
        
        # Crear lotes
        batches = [all_doc_ids[i:i + BATCH_SIZE] 
                   for i in range(0, len(all_doc_ids), BATCH_SIZE)]
        
        logger.info(f"Total lotes: {len(batches)} | Tamaño lote: {BATCH_SIZE}")
        
        # Procesar lotes en paralelo con semáforo para controlar concurrencia
        semaphore = asyncio.Semaphore(MAX_CONCURRENT_BATCHES)
        
        async def process_with_semaphore(batch):
            async with semaphore:
                return await process_batch(batch)
        
        # Crear tareas para todos los lotes
        tasks = [process_with_semaphore(batch) for batch in batches]
        
        # Ejecutar tareas en paralelo y monitorear progreso
        total_updated = 0
        processed = 0
        
        for coro in asyncio.as_completed(tasks):
            updated = await coro
            total_updated += updated
            processed += BATCH_SIZE
            logger.info(f"Progreso: min({processed}, {len(all_doc_ids)})/{len(all_doc_ids)} docs | "
                      f"Actualizados: {total_updated}")
        
        # Resultados finales
        logger.info("\n=== PROCESO COMPLETADO ===")
        logger.info(f"Documentos procesados: {len(all_doc_ids)}")
        logger.info(f"Documentos actualizados: {total_updated}")
        
    except Exception as e:
        logger.error(f"Error fatal: {e}")

# Versión simple con cliente asíncrono (recomendada)
async def add_formatted_datetime_simple():
    """Versión simple usando cliente asíncrono con streaming"""
    logger.info("Iniciando proceso simple asíncrono...")
    
    # Obtener referencia a la colección
    collection_ref = db.collection(COLLECTION_NAME)
    
    # Stream asíncrono de documentos
    docs = collection_ref.stream()
    
    batch = db.batch()
    batch_count = 0
    updated_count = 0
    doc_count = 0
    
    async for doc in docs:
        doc_count += 1
        reading_datetime = doc.get('READING_DATETIME')
        
        if reading_datetime and isinstance(reading_datetime, str):
            # Conversión directa por posición
            try:
                day = int(reading_datetime[0:2])
                month = int(reading_datetime[3:5])
                year = int(reading_datetime[6:10])
                hour = int(reading_datetime[10:12])
                minute = int(reading_datetime[13:15])
                
                dt = datetime(year, month, day, hour, minute)
                
                # Actualizar documento
                doc_ref = collection_ref.document(doc.id)
                batch.update(doc_ref, {
                    'FORMATED_READING_DATETIME': dt
                })
                updated_count += 1
                
            except Exception as e:
                logger.error(f"Error en doc {doc.id}: {reading_datetime} - {e}")
        
        # Commit cada 500 documentos
        if doc_count % BATCH_SIZE == 0:
            await batch.commit()
            batch = db.batch()
            batch_count += 1
            logger.info(f"Procesados {doc_count} documentos | Actualizados: {updated_count}")
    
    # Commit final
    if doc_count % BATCH_SIZE != 0:
        await batch.commit()
    
    logger.info(f"\n✅ Proceso completado: {updated_count} documentos actualizados de {doc_count}")

# Versión optimizada para 8000 documentos (usa paginación y paralelismo)
async def add_formatted_datetime_optimized():
    """Versión optimizada con paginación y procesamiento paralelo"""
    logger.info("Iniciando proceso optimizado asíncrono...")
    
    collection_ref = db.collection(COLLECTION_NAME)
    total_updated = 0
    total_processed = 0
    
    # Procesar por páginas de 2000 documentos
    page_size = 2000
    last_doc = None
    
    while True:
        # Obtener página de documentos
        query = collection_ref.limit(page_size)
        if last_doc:
            query = query.start_after(last_doc)
        
        docs = await query.get()
        
        if not docs:
            break
        
        # Procesar documentos de la página en batches de 500
        for i in range(0, len(docs), BATCH_SIZE):
            batch_docs = docs[i:i + BATCH_SIZE]
            batch = db.batch()
            batch_updated = 0
            
            for doc in batch_docs:
                reading_datetime = doc.get('READING_DATETIME')
                
                if reading_datetime and isinstance(reading_datetime, str):
                    try:
                        # Conversión rápida por posición
                        dt = datetime(
                            int(reading_datetime[6:10]),  # año
                            int(reading_datetime[3:5]),   # mes
                            int(reading_datetime[0:2]),   # día
                            int(reading_datetime[10:12]), # hora
                            int(reading_datetime[13:15])  # minuto
                        )
                        
                        batch.update(doc.reference, {
                            'FORMATED_READING_DATETIME': dt
                        })
                        batch_updated += 1
                        
                    except Exception as e:
                        logger.error(f"Error en doc {doc.id}: {e}")
            
            if batch_updated > 0:
                await batch.commit()
                total_updated += batch_updated
            
            total_processed += len(batch_docs)
            logger.info(f"Progreso: {total_processed} documentos | Actualizados: {total_updated}")
        
        last_doc = docs[-1]
    
    logger.info(f"\n✅ Proceso completado: {total_updated} documentos actualizados de {total_processed}")

# Función para verificar el formato
async def verify_format():
    """Verificar que todos los documentos tengan el formato esperado"""
    docs = db.collection(COLLECTION_NAME).limit(10).stream()
    
    print("Verificando formato de los primeros 10 documentos:")
    async for doc in docs:
        reading = doc.get('READING_DATETIME')
        if reading:
            print(f"Doc {doc.id}: '{reading}' - Longitud: {len(reading)}")
            if len(reading) == 16:  # "DD/MM/YYYYHH:MM:" son 16 caracteres
                print(f"  ✅ Formato correcto")
            else:
                print(f"  ⚠️ Longitud inesperada: {len(reading)}")

# Función principal
async def main():
    """Función principal que coordina todo"""
    try:
        # 1. Verificar formato
        await verify_format()
        
        print("\n" + "="*50)
        
        # 2. Elegir método (descomentar el que prefieras)
        
        # Método 1: Simple y directo (recomendado para la mayoría)
        #await add_formatted_datetime_simple()
        
        # Método 2: Optimizado con paginación (bueno para 8000 docs)
        # await add_formatted_datetime_optimized()
        
        # Método 3: Paralelo avanzado (máxima velocidad)
        await add_formatted_datetime_parallel()
        
    except Exception as e:
        logger.error(f"Error en main: {e}")
    finally:
        # Cerrar el cliente asíncrono
        db.close()

# Ejecutar el script
if __name__ == "__main__":
    # Ejecutar la función principal asíncrona
    asyncio.run(main())