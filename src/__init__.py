import csv
import logging
import os

import psycopg2
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(
    format=f"[%(asctime)s] [{os.getpid()}] [%(levelname)s] - %(name)s: %(message)s",
    level=logging.INFO,
    datefmt="%Y/%m/%d %H:%M:%S %z",
)


logger = logging.getLogger(__file__)

host = os.environ.get("PGSQL_HOST")
user = os.environ.get("PGSQL_USER")
dbname = os.environ.get("PGSQL_DBNAME")
password = os.environ.get("PGSQL_PASSWORD")

# See https://docs.microsoft.com/en-us/azure/postgresql/connect-python
conn = psycopg2.connect(f"host={host} user={user} dbname={dbname} password={password}")

logging.info(f"Connected to {host} - {dbname}")


dir_path = os.path.dirname(os.path.realpath(__file__))

SCHEMA = {
    "Month": {
        "attributes": {
            "month_key": "SERIAL PRIMARY KEY",
            "code": "INT NOT NULL",
            "name": "VARCHAR(9) NOT NULL",
            "quarter": "INT NOT NULL",
            "year": "INT NOT NULL",
            "decade": "INT NOT NULL",
        },
        "rules": ["CHECK (quarter BETWEEN 1 AND 4)", "CHECK (code BETWEEN 1 AND 12)"],
    },
    "Country": {
        "attributes": {
            "country_key": "SERIAL PRIMARY KEY",
            "name": "VARCHAR(20) NOT NULL",
            "code": "VARCHAR(3) NOT NULL",
            "region": "VARCHAR(255) NOT NULL",
            "currency": "VARCHAR(255) NOT NULL",
            "income_group": "VARCHAR(255) NOT NULL",
        },
        "indicators": {
            "SP.POP.TOTL": {"name": "population", "type": "INT"},
            "SP.DYN.CBRT.IN": {"name": "crude_birth_rate", "type": "FLOAT"},
            "SP.DYN.CDRT.IN": {"name": "crude_death_rate", "type": "FLOAT"},
            "SL.TLF.TOTL.IN": {"name": "labor_force", "type": "INT"},
            "SP.DYN.LE00.IN": {"name": "life_expectancy", "type": "FLOAT"},
            "SP.DYN.LE00.MA.IN": {"name": "life_expectancy_male", "type": "FLOAT"},
            "SP.DYN.LE00.FE.IN": {"name": "life_expectancy_female", "type": "FLOAT"},
        },
        "rules": [],
    },
    # TODO
    # "Education": {
    #     "attributes": {
    #         "education_key": "SERIAL PRIMARY KEY",
    #     },
    #     "indicators": {}
    #     "rules": ""
    # },
    "WB_HNP": {
        "attributes": {
            "id": "SERIAL PRIMARY KEY",
            "month_key": "SERIAL",
            "country_key": "SERIAL",
            # TODO: more PFKs & measures
        },
        "rules": [
            """
                CONSTRAINT fk_month FOREIGN KEY(month_key) 
                    REFERENCES month(month_key)
	                ON DELETE SET NULL
            """,
            """
            CONSTRAINT fk_country
                FOREIGN KEY(country_key) 
	                REFERENCES country(country_key)
	                ON DELETE SET NULL
            """,
        ],
    },
}

indicators = set()
for name, table in SCHEMA.items():
    for ind in table.get("indicators", {}).keys():
        indicators.add(ind)

statsData = []
with open(
    f"{dir_path}/../csv/attributes/HNP_StatsData.csv",
    newline="",
    mode="r",
    encoding="utf-8-sig",
) as StatsDataCSV:
    readerStatsData = csv.DictReader(StatsDataCSV)
    for row in readerStatsData:
        if row["Indicator Code"] in indicators:
            statsData.append(row)

logging.info(f"Indicator Count: {len(indicators)}")
logging.info(f"StatsData Row Count: {len(statsData)}")
