import os
import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv(r"C:\Users\alex\Desktop\ai_countries\.env")

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

        # respuesta vacia o no hay campo de datos o el campo de datos vacio
        # no capturado por raise_for_status - propio de WB
        if not data or len(data) < 2 or data[1] is None:
                break

        # esto es transform, desacoplar para mas adelante
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




def ejecutar_peticion_wb(indicator, pagination) -> list[dict]:
    params = {
        "format": "json",
        "date": "2019:2024",
        "per_page": 500,
        "page": pagination
    }

    last_exc: Exception | None = None
    for attempt in range(MAX_RETRIES):
        try:
            r = requests.get(BASE_URL + f"/country/all/indicator/{indicator}", params=params, timeout=REQUEST_TIMEOUT)
            r.raise_for_status()
            data = r.json()
            return data[0], data[1]
        except requests.exceptions.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else None
            if status not in RETRYABLE_STATUS:
                # 4xx - determinista: no reintentar
                logger.error("WorldBank respondio %s (no recuperable): %s", status, exc)
                raise
            last_exc = exc
            logger.warning("WorldBank %s  (intento %s/%s)",
                status, attempt + 1, MAX_RETRIES)
    
        except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
            last_exc = exc
            logger.warning("Fallo de red en %s (intento %s/%s): %s",
                code, attempt + 1, MAX_RETRIES, exc)
    
        if attempt < MAX_RETRIES - 1:   
            wait = BACKOFF_BASE ** attempt + uniform(0, 0.5)
            logger.info("Reintentando en %ss...", wait)
            time.sleep(wait)

        # este else se ejecuta si no se produce nunca break en el FOR - todos los intentos fallaron
        else:
            raise RuntimeError(
                "WorldBank no respondió tras %s intentos" %(MAX_RETRIES)
            ) from last_exc


def ejecutar_paginacion_wb(indicator, page=1) -> list[dict]:
    rows = []
    while True:
        row0, row1 = ejecutar_peticion_wb(indicator, pagination=page)
        if not row1 or len(row1)<2 or row1 is None:
            break
        rows.extend(row1)

        if page >= row0["pages"]: break
        page += 1

def main() -> None:
    config.configure_logging()

    indicators = {
        "SP.POP.TOTL": "POPULATION",
        "NY.GDP.MKTP.CD": "GDP_USD",
        "GB.XPD.RSDV.GD.ZS": "RD_GDP_PCT",
        "SP.POP.SCIE.RD.P6": "RESEARCHERS_PER_MILLION",
        "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD"
    }

    all_data = []

    for indicator, name in indicators.items():
        data_indic = ejecutar_paginacion_wb(indicator)
        all_data.extend(data_indic)

        # la escritura dentro del bucle para diferenciar archivos
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"datos-{name}-{datetime.date.today().strftime('%Y%m%d')}.json"
        with open(output_path, "w") as f:
            for record in all_data:
                f.write(json.dumps(record) + "\n")

    logger.info("Done. %s registros → %s", len(all_data), output_path)

if __name__ == "__main__":
    main()


def ejecutar_peticion_wb(indicator, page=1) -> list[dict]:
    params = {
        "format": "json", # si no, devuelve xml
        "date": "2019:2024",
        "per_page": 500,
        "page": page
    }

    rows = []

    while True:
        """
        Bucle externo desde el cual un indicator podra producir una respuesta
        de varias paginas 
        """
        last_exc: Exception | None = None
        for attempt in range(MAX_RETRIES):
            """
            Bucle interno que recorrera las paginas disponibles mediante distintas
            peticiones acorde a la paginacion - incremental
            """
            try:
                r = requests.get(BASE_URL + f"/country/all/indicator/{indicator}", params=params, timeout=REQUEST_TIMEOUT)
                r.raise_for_status()
                data = r.json()
                break

            except requests.exceptions.HTTPError as exc:
                status = exc.response.status_code if exc.response is not None else None
                if status not in RETRYABLE_STATUS:
                    # 4xx - determinista: no reintentar
                    logger.error("WorldBank respondio %s (no recuperable): %s", status, exc)
                    raise
                last_exc = exc
                logger.warning("WorldBank %s  (intento %s/%s)",
                    status, attempt + 1, MAX_RETRIES)
        
            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as exc:
                last_exc = exc
                logger.warning("Fallo de red en %s (intento %s/%s): %s",
                    code, attempt + 1, MAX_RETRIES, exc)
        
            if attempt < MAX_RETRIES - 1:   
                wait = BACKOFF_BASE ** attempt + uniform(0, 0.5)
                logger.info("Reintentando en %ss...", wait)
                time.sleep(wait)

        # este else se ejecuta si no se produce nunca break en el FOR - todos los intentos fallaron
        else:
            raise RuntimeError(
                "WorldBank no respondió tras %s intentos" %(MAX_RETRIES)
            ) from last_exc
        
        # respuesta vacia o no hay campo de datos o el campo de datos vacio
        # no capturado por raise_for_status - propio de WB
        if not data or len(data) < 2 or data[1] is None:
                break
        # si se guarda antes no se comprueba si la pagina estaba vacia u otro error
        rows.extend(data[1]) # si se usa append seria lista de listas no lista de dicts (registros)
        if params["page"] >= data[0]["pages"]: break
        params["page"] += 1
    
    return rows

def main() -> None:
    config.configure_logging()

    indicators = {
        "SP.POP.TOTL": "POPULATION",
        "NY.GDP.MKTP.CD": "GDP_USD",
        "GB.XPD.RSDV.GD.ZS": "RD_GDP_PCT",
        "SP.POP.SCIE.RD.P6": "RESEARCHERS_PER_MILLION",
        "NY.GDP.PCAP.CD": "GDP_PER_CAPITA_USD"
    }

    all_data = []

    for indicator, name in indicators.items():
        data_indic = ejecutar_peticion_wb(indicator)
        all_data.extend(data_indic)

        # la escritura dentro del bucle para diferenciar archivos
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        output_path = OUTPUT_DIR / f"datos-{name}-{datetime.date.today().strftime('%Y%m%d')}.json"
        with open(output_path, "w") as f:
            for record in all_data:
                f.write(json.dumps(record) + "\n")

    logger.info("Done. %s registros → %s", len(all_data), output_path)

if __name__ == "__main__":
    main()