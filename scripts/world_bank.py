import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv("../.env")

indicators = {
    "SP.POP.TOTL": "POPULATION",
    "NY.GDP.MKTP.CD": "GDP_USD",
    "GB.XPD.RSDV.GD.ZS": "RD_GDP_PCT",
    "SP.POP.SCIE.RD.P6": "RESEARCHERS_PER_MILLION",
    "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD"
}

wb_aggregates = [
    "AFE","AFW","ARB","CEB","CSS","EAP","EAR","EAS","ECA","ECS",
    "EMU","EUU","FCS","HIC","HPC","IBD","IBT","IDA","IDB","IDX",
    "LAC","LCN","LDC","LIC","LMC","LMY","LTE","MEA","MIC","MNA",
    "NAC","OED","OSS","PRE","PSS","PST","SAS","SSA","SSF","SST",
    "TEA","TEC","TLA","TMN","TSA","TSS","UMC","WLD","XZN"
]

rows = []
for code, name in indicators.items():
    page = 1
    while True:
        url = f"https://api.worldbank.org/v2/country/all/indicator/{code}"
        
        resp = requests.get(url, params={
            "format": "json", "date": "2019:2024",
            "per_page": 500, "page": page
        })

        if resp.status_code != 200 or not resp.text:
            print(f"Error en {name}, page {page}: status {resp.status_code}")
            break
        data = resp.json()

        if not data or len(data) < 2 or data[1] is None:
            break

        for r in data[1]:
            if r["value"] is None: continue
            cid = r["countryiso3code"]
            if not cid or len(cid) != 3 or cid in wb_aggregates: continue
            rows.append({
                "COUNTRY_ID": cid,
                "COUNTRY_NAME": r["country"]["value"],
                "YEAR": int(r["date"]),
                "INDICATOR": name,
                "VALUE": r["value"]
            })
        if page >= data[0]["pages"]: break
        page += 1
    print(f"{name}: done")

df_wb = pd.DataFrame(rows)
print(df_wb.shape)
print(df_wb.groupby("INDICATOR").size())

conn = snowflake.connector.connect(
    account=os.environ["SNOWFLAKE_ACCOUNT"],
    user=os.environ["SNOWFLAKE_USER"],
    password=os.environ["SNOWFLAKE_PASSWORD"],
    database=os.environ["SNOWFLAKE_DATABASE"],
    schema="RAW",
    warehouse=os.environ["SNOWFLAKE_WAREHOUSE"]
)

success, nchunks, nrows, _ = write_pandas(conn, df_wb, "WORLDBANK_INDICATORS")
print(f"Cargadas: {nrows} filas")
conn.close()