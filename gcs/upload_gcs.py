import logging

from pathlib import Path
from ingestion import config
from google.api_core import exceptions as gcp_exceptions
from google.cloud import storage

logger = logging.getLogger(__name__)

BUCKET_NAME = config.BUCKET

def upload_to_gcs(client: storage.Client, local_path: str, destination_blob: str) -> None:
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob)

    try:
        blob.upload_from_filename(local_path)
    except gcp_exceptions.GoogleAPIError:
        logger.exception("Fallo subiendo %s a gs://%s/%s",
                         local_path, BUCKET_NAME, destination_blob)
        raise
    logger.info("Subido: %s -> gs://%s/%s", local_path, BUCKET_NAME, destination_blob)

# conviene testear, es logica pura no llamada a API de google
def buscar_archivos(file_path: Path, client: storage.Client, dest_blob: str) -> None:
    # Path en vez de glob - antigua
    files = list(file_path.glob("*.json"))
    if not files:
        logger.warning("No hay archivos en %s/", file_path)
        return
    
    for f in files:
        #filename = os.path.basename(f) - f ya es objeto path -> extraer directamente
        upload_to_gcs(client, str(f), f"{dest_blob}/{f.name}")
    logger.info("Archivos subidos a GCS")

def main() -> None:
    config.configure_logging()
    client = storage.Client(config.PROJECT_ID)

    # OpenAlex
    buscar_archivos(config.OA_DIR, client, "openalex")
    
    # WorldBank
    buscar_archivos(config.WB_DIR, client, "worldbank")

if __name__ == "__main__":
    main()