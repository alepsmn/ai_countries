import os
import pycountry
from google.cloud import bigquery
import snowflake.connector
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv("../.env")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.environ["GCP_CREDENTIALS_PATH"]

client = bigquery.Client(project="iniciocloud")

query = """
SELECT
    country_code,
    CAST(SUBSTR(CAST(publication_date AS STRING), 1, 4) AS INT64) AS year,
    COUNT(*) AS ai_patents
FROM `patents-public-data.patents.publications`,
    UNNEST(cpc) AS cpc_code
WHERE
    cpc_code.code LIKE 'G06N%'
    AND CAST(SUBSTR(CAST(publication_date AS STRING), 1, 4) AS INT64) BETWEEN 2019 AND 2024
    AND country_code IS NOT NULL
    AND country_code != ''
GROUP BY country_code, year
ORDER BY year, ai_patents DESC
"""

df = client.query(query).to_dataframe()

def iso2_to_country(code):
    try:
        c = pycountry.countries.get(alpha_2=code)
        return c.alpha_3, c.name
    except:
        return None, None

df = df[~df["country_code"].isin(["WO", "EP"])]
df[["COUNTRY_ID", "COUNTRY_NAME"]] = df["country_code"].apply(
    lambda x: pd.Series(iso2_to_country(x))
)
df = df.dropna(subset=["COUNTRY_ID"])
df["INDICATOR"] = "AI_PATENTS"
df = df.rename(columns={"year": "YEAR", "ai_patents": "VALUE"})
df = df[["COUNTRY_ID", "COUNTRY_NAME", "YEAR", "INDICATOR", "VALUE"]]

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="RAW",
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]
)

success, nchunks, nrows, _ = write_pandas(conn, df, "BIGQUERY_PATENTS")
print(f"Cargadas: {nrows} filas")
conn.close()