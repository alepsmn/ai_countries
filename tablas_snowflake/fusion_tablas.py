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

corea = [

    # DATOS KOREA SUR MANUAL 1
    """
    INSERT INTO IA_PAISES.RAW.WORLDBANK_INDICATORS VALUES
    ('KOR', 'Korea, Rep.', 2019, 'RESEARCHERS_PER_MILLION', 8328.96727),
    ('KOR', 'Korea, Rep.', 2020, 'RESEARCHERS_PER_MILLION', 8620.009),
    ('KOR', 'Korea, Rep.', 2021, 'RESEARCHERS_PER_MILLION', 9071.4503),
    ('KOR', 'Korea, Rep.', 2022, 'RESEARCHERS_PER_MILLION', 9434.7613),
    ('KOR', 'Korea, Rep.', 2023, 'RESEARCHERS_PER_MILLION', 9471.82543)""",

    # ESTANDARAZAR NOMBRE KOREA DEL SUR DE TABLAS RAW 2
    """UPDATE IA_PAISES.RAW.OPENALEX_PUBLICATIONS
    SET COUNTRY_NAME = 'Korea, Rep.'
    WHERE COUNTRY_NAME = 'Korea, Republic of'""",

    """UPDATE IA_PAISES.RAW.BIGQUERY_PATENTS
    SET COUNTRY_NAME = 'Korea, Rep.'
    WHERE COUNTRY_NAME = 'Korea, Republic of'""",
]

for sql in corea:
    cur.execute(sql)
    print(f"OK: {sql[:60].strip()}")

# validación
cur.execute(    #-- VISTA
    """SELECT * FROM IA_PAISES.ANALYTICS.VW_AI_KPI
    WHERE COUNTRY_ID = 'KOR'
    ORDER BY YEAR""")
for row in cur.fetchall():
    print(row)

cur.close()
conn.close()