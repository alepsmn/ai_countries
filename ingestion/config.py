import logging
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()


# --- Credencials / GCPlatform -----------------
# La autenticacion va por ADC (Application Default Credentials): la organizacion
# prohibe crear claves de service account (iam.disableServiceAccountKeyCreation).
# Se obtienen con `gcloud auth application-default login`; los clientes de Google
# las resuelven solos, sin pasar ningun fichero de credenciales.
OPENALEX_API_KEY: str | None = os.getenv("OPENALEX_API_KEY")
USER_MAIL: str | None = os.getenv("USER_MAIL")
PROJECT_ID: str = os.getenv("GCP_PROJECT_ID", "ai-countries-501514")

# --- Destinos GCService ---------------------
BUCKET: str = os.getenv("GCS_BUCKET", "ai_countries_raw")
DATASET: str = os.getenv("BQ_DATASET", "raw")
# El bucket es multi-region US: el dataset debe compartir location o el load job falla.
BQ_LOCATION: str = os.getenv("BQ_LOCATION", "US")

# --- Rutas locales (landing crudo) ----------
DATA_DIR: Path = Path(os.getenv("DATA_DIR", "data/raw"))
OA_DIR: Path = DATA_DIR / "openalex"
WB_DIR: Path = DATA_DIR / "worldbank"

# --- Rango temporal -------------------------
# 2023 es el ultimo ano completo en OA: 2024 sigue recibiendo
# indexaciones y compararlo con 2023 sesgaria a la baja
YEAR_START: int = int(os.getenv("YEAR_START", "2019"))
YEAR_END: int = int(os.getenv("YEAR_END", "2023")) # inclusive
YEARS: range = range(YEAR_START, YEAR_END + 1)

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