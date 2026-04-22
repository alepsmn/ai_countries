import os
import requests
import pycountry
import snowflake.connector
from dotenv import load_dotenv

load_dotenv("../.env")

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="RAW",
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]
)
cur = conn.cursor()

def iso2_to_country(code):
    try:
        c = pycountry.countries.get(alpha_2=code)
        return c.alpha_3, c.name
    except:
        return None, None

url = "https://api.openalex.org/works"

for year in range(2019, 2024):
    params = {
        "filter": f"concepts.id:C154945302,from_publication_date:{year}-01-01,to_publication_date:{year}-12-31",
        "group_by": "authorships.countries",
        "per_page": 200,
        "mailto": "alepsmn@gmail.com"
    }
    r = requests.get(url, params=params)
    data = r.json()

    rows = []
    for item in data.get("group_by", []):
        iso2 = item["key"].split("/")[-1]
        iso3, name = iso2_to_country(iso2)
        if iso3:
            rows.append((iso3, name, year, "AI_PUBLICATIONS", item["count"]))

    cur.executemany(
        "INSERT INTO OPENALEX_PUBLICATIONS (COUNTRY_ID, COUNTRY_NAME, YEAR, INDICATOR, VALUE) VALUES (%s,%s,%s,%s,%s)",
        rows
    )
    conn.commit()
    print(f"{year}: {len(rows)} filas")

cur.close()
conn.close()