import logging

from pathlib import Path
from ingestion import config, validacion
from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage

logger = logging.getLogger(__name__)

BUCKET_NAME = config.BUCKET

def upload_to_gcs(client: storage.Client, local_path: str, destination_blob: str) -> None:
    bucket = client.bucket(BUCKET_NAME) # type: ignore[no-untyped-call]
    blob = bucket.blob(destination_blob)

    try:
        blob.upload_from_filename(local_path)
    except gcp_exceptions.GoogleAPIError:
        logger.exception("Fallo subiendo %s a gs://%s/%s",
                         local_path, BUCKET_NAME, destination_blob)
        raise
    logger.info("Subido: %s -> gs://%s/%s", local_path, BUCKET_NAME, destination_blob)

def subir_archivos(files: list[Path], client: storage.Client, dest_blob: str) -> None:
    for f in files:
        #filename = os.path.basename(f) - f ya es objeto path -> extraer directamente
        upload_to_gcs(client, str(f), f"{dest_blob}/{f.name}")
    logger.info("%s archivos pasan la puerta desde %s", len(files), dest_blob)

# conviene testear, es logica pura no llamada a API de google
def validar_archivos(file_path:Path) -> list[Path]:
    files = list(file_path.glob("*.json"))
    if not files:
        # solo los logs son de evaluacion perezosa %s", sustituto, f-string en lo demas
        raise validacion.PuertaError(f"No hay archivos en {file_path}")
    
    for f in files:
        validacion.valida(f)
    logger.info("%s archivos pasan la puerta desde %s", len(files), file_path)
    return files
        
def main() -> None:
    config.configure_logging()
    client = storage.Client(config.PROJECT_ID)# type: ignore[no-untyped-call]


    archivos_listos_oa = validar_archivos(config.OA_DIR)
    archivos_listos_wb = validar_archivos(config.WB_DIR)
    
    subir_archivos(archivos_listos_oa, client, "openalex")
    subir_archivos(archivos_listos_wb, client, "worldbank")

if __name__ == "__main__":
    main()