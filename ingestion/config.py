import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# --- Credencials / GCPlatform -----------------
OPENALEX_API_KEY: str | None = os.getenv("OPENALEX_API_KEY")
USER_MAIL: str | None = os.getenv("USER_MAIL")
PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "ai-countries-501514")
KEY_FILE: str = os.getenv("GCP_KEYFILE", "keyfile.json")

# --- Destinos GCService ---------------------
BUCKET: str = os.getenv("GCS_BUCKET", "datacenter-impact-raw")
# DATASET: str = os.getenv()
BQ_LOCATION: str = os.getenv("BQ_LOCATION", "US")

# --- Rutas locales (landing crudo) ----------
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data/raw"))
OA_DIR: Path = DATA_DIR / "openalex"
WB_DIR: Path = DATA_DIR / "worldbank"

# --- APIS (publicas) ------------------------
OA_BASE_URL: str = "https://api.openalex.org/works"
WB_BASE_URL: str = "https://api.worldbank.org/v2"

def configure_logging(level: int = logging.INFO) -> None:
    """ Configura el logging raiz con timestamps y nivel
    Idempotente entre scripts: cada 'main()' la invoca una vez al arrancar
    en lugar de usar 'print()'. 'force=True' reemplaza handlers previos
    (util bajo Airflow, que ya configura su propio root logger).
    """
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M%S",
        force=True
    )