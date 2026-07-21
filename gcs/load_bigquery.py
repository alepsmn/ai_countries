"""
Esquemas de las tablas RAW de BigQuery.

La capa RAW es un espejo fiel del fichero crudo: los tipos declarados aqui son
los que la API devuelve, no los que nos gustaria tener. Las conversiones
(`date` a entero, normalizacion de nombres de pais) pertenecen a dbt/staging.
Declarar aqui un tipo "mejor" que el real hace que el load falle o, peor, que
BigQuery lo acepte y el fichero y la tabla dejen de coincidir.
"""

import logging

from ingestion import config
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

logger = logging.getLogger(__name__)

PROJECT_ID = config.PROJECT_ID
DATASET = config.DATASET
BUCKET = config.BUCKET

# Envoltura comun a las dos fuentes: la escriben los modulos de landing
# (`ingestion/*_landing.py`), no las APIs. Solo cambia `payload`.
ENVOLTURA = [
    bigquery.SchemaField("source", "STRING", mode="REQUIRED"),
    bigquery.SchemaField("request_url", "STRING"),
    bigquery.SchemaField("year", "INTEGER", mode="REQUIRED"),
    bigquery.SchemaField("ingested_at", "TIMESTAMP", mode="REQUIRED"),
]

# --- World Bank -------------------------------------------------------------
# Un registro por fila: cada linea del NDJSON es un par pais-indicador-anio.
#
# `date` va como STRING a proposito: World Bank lo manda entrecomillado
# ("2023") y BigQuery no convierte string a INT64 en un load de JSON.
# `value` es FLOAT64 porque llega como entero, decimal o null segun indicador.

WB_SCHEMA = ENVOLTURA + [
    bigquery.SchemaField(
        "payload",
        "RECORD",
        mode="REQUIRED",
        fields=[
            bigquery.SchemaField(
                "indicator",
                "RECORD",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("value", "STRING"),
                ],
            ),
            bigquery.SchemaField(
                "country",
                "RECORD",
                fields=[
                    bigquery.SchemaField("id", "STRING"),
                    bigquery.SchemaField("value", "STRING"),
                ],
            ),
            bigquery.SchemaField("countryiso3code", "STRING"),
            bigquery.SchemaField("date", "STRING"),
            bigquery.SchemaField("value", "FLOAT"),
            bigquery.SchemaField("unit", "STRING"),
            bigquery.SchemaField("obs_status", "STRING"),
            bigquery.SchemaField("decimal", "INTEGER"),
        ],
    ),
]

# --- OpenAlex ---------------------------------------------------------------
# Una fila por anio: el fichero entero es un objeto y `payload.group_by` trae
# los ~200 paises dentro. De ahi el mode="REPEATED": un array de structs, no
# un struct anidado.
#
# `meta.x_query` no se modela campo a campo. Dentro lleva `oqo.filter_rows`,
# una lista donde `value` es a veces entero y a veces string: no hay un tipo
# columnar que sea cierto para ambos. Como tipo JSON se guarda entero (la capa
# RAW debe ser reproducible) y se consulta con JSON_VALUE si algun dia importa.

OA_SCHEMA = ENVOLTURA + [
    bigquery.SchemaField(
        "payload",
        "RECORD",
        mode="REQUIRED",
        fields=[
            bigquery.SchemaField(
                "meta",
                "RECORD",
                fields=[
                    bigquery.SchemaField("count", "INTEGER"),
                    bigquery.SchemaField("db_response_time_ms", "INTEGER"),
                    bigquery.SchemaField("page", "INTEGER"),
                    bigquery.SchemaField("per_page", "INTEGER"),
                    bigquery.SchemaField("groups_count", "INTEGER"),
                    bigquery.SchemaField("cost_usd", "FLOAT"),
                    bigquery.SchemaField("x_query", "JSON"),
                ],
            ),
            bigquery.SchemaField(
                "group_by",
                "RECORD",
                mode="REPEATED",
                fields=[
                    bigquery.SchemaField("key", "STRING"),
                    bigquery.SchemaField("key_display_name", "STRING"),
                    bigquery.SchemaField("count", "INTEGER"),
                ],
            ),
        ],
    ),
]

def load_table(
        client: bigquery.Client,
        gcs_uri: str,
        table_ref: str,
        schema: list[bigquery.SchemaField]
) -> None:
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        ignore_unknown_values=False
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()

        filas_cargadas = load_job.output_rows
        logger.info("Exito, filas cargadas: %s", filas_cargadas)
    except gcp_exceptions.GoogleAPIError:
        logger.exception("Fallo cargando %s -> %s", gcs_uri, table_ref)
        raise

def main() -> None:
    config.configure_logging()

    client = bigquery.Client(project=PROJECT_ID)
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{DATASET}")
    dataset_ref.location = config.BQ_LOCATION
    client.create_dataset(dataset_ref, exists_ok=True)
    logger.info("Dataset %s listo.", DATASET)

    load_table(
        client,
        f"gs://{BUCKET}/openalex/*.json",
        f"{PROJECT_ID}.{DATASET}.openalex",
        OA_SCHEMA
    )

    load_table(
        client,
        f"gs://{BUCKET}/worldbank/*.json",
        f"{PROJECT_ID}.{DATASET}.worldbank",
        WB_SCHEMA
    )

if __name__ == "__main__":
    main()