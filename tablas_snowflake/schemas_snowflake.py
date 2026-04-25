import os
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("../.env")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]
)
cur = conn.cursor()

statements  = [

    # 1. BBDD
    "USE ROLE ACCOUNTADMIN",
    "CREATE DATABASE IF NOT EXISTS IA_PAISES",
    "USE DATABASE IA_PAISES",

    # 2. SCHEMAS
    "CREATE SCHEMA IF NOT EXISTS IA_PAISES.RAW",
    "CREATE SCHEMA IF NOT EXISTS IA_PAISES.ANALYTICS",

    # 3.TABLAS RAW (DROP + CREATE)
    "DROP TABLE IF EXISTS IA_PAISES.RAW.OPENALEX_PUBLICATIONS",
    """CREATE TABLE IA_PAISES.RAW.OPENALEX_PUBLICATIONS(
        COUNTRY_ID VARCHAR(20),
        COUNTRY_NAME VARCHAR(100),
        YEAR INT,
        INDICATOR VARCHAR(50),
        VALUE FLOAT
    )""",

    "DROP TABLE IF EXISTS IA_PAISES.RAW.WORLDBANK_INDICATORS",
    """CREATE TABLE IA_PAISES.RAW.WORLDBANK_INDICATORS(
        COUNTRY_ID VARCHAR(20),
        COUNTRY_NAME VARCHAR(100),
        YEAR INT,
        INDICATOR VARCHAR(50),
        VALUE FLOAT
    )""",

    "DROP TABLE IF EXISTS IA_PAISES.RAW.BIGQUERY_PATENTS",
    """CREATE TABLE IA_PAISES.RAW.BIGQUERY_PATENTS(
        COUNTRY_ID VARCHAR(20),
        COUNTRY_NAME VARCHAR(100),
        YEAR INT,
        INDICATOR VARCHAR(50),
        VALUE FLOAT
    )""",

    # 4. TABLA FUTURA PARA LOS BLOQUES GEOPOLITICOS
    "DROP TABLE IF EXISTS IA_PAISES.RAW.DIM_COUNTRIES",
    """CREATE TABLE IA_PAISES.RAW.DIM_COUNTRIES(
        COUNTRY_ID VARCHAR(20),
        COUNTRY_NAME VARCHAR(100),
        BLOC VARCHAR(20),
        IS_NATO BOOLEAN)"""
]

for sql in statements:
    cur.execute(sql)
    print(f"OK: {sql[:60].strip()}")

cur.execute("SHOW TABLES IN SCHEMA IA_PAISES.RAW")
tables = [row[1] for row in cur.fetchall()]
print(tables)

cur.close()
conn.close()