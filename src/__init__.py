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
            "Primary Key": {"name": "month_key", "type": "SERIAL PRIMARY KEY"},
            "Code": {"name": "code", "type": "INT NOT NULL"},
            "Name": {"name": "name", "type": "VARCHAR(9) NOT NULL"},
            "Quarter": {"name": "quarter", "type": "INT NOT NULL"},
            "Year": {"name": "year", "type": "INT NOT NULL"},
            "Decade": {"name": "decade", "type": "INT NOT NULL"},
        },
        "rules": ["CHECK (quarter BETWEEN 1 AND 4)", "CHECK (code BETWEEN 1 AND 12)"],
    },
    "Country": {
        "attributes": {
            "Primary Key": {
                "name": "country_key",
                "type": "SERIAL PRIMARY KEY",
            },
            "Short Name": {"name": "name", "type": "VARCHAR(20) NOT NULL"},
            "Country Code": {"name": "code", "type": "VARCHAR(3) NOT NULL"},
            "Region": {"name": "region", "type": "VARCHAR(255) NOT NULL"},
            "Currency Unit": {"name": "currency", "type": "VARCHAR(255) NOT NULL"},
            "Income Group": {"name": "income_group", "type": "VARCHAR(255) NOT NULL"},
            "SP.POP.TOTL": {"name": "population", "type": "INT"},
            "SP.POP.GROW": {"name": "population_growth", "type": "FLOAT"},
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
    #        "Primary Key": {
    #             "name": "education_key",
    #             "type": "SERIAL PRIMARY KEY",
    #         },
    #     },
    #     "rules": ""
    # },
    "QualityOfLife": {
        "attributes": {
            "Primary Key": {"name": "qualityoflife_key", "type": "SERIAL PRIMARY KEY"},
            "SL.UEM.TOTL.ZS": {"name": "unemployment_rate", "type": "FLOAT"},
            "SL.UEM.TOTL.MA.ZS": {"name": "unemployment_rate_male", "type": "FLOAT"},
            "SL.UEM.TOTL.FE.ZS": {"name": "unemployment_rate_female", "type": "FLOAT"},
            "SH.STA.SUIC.P5": {"name": "suicide_rate", "type": "FLOAT"},
            "SH.STA.SUIC.MA.P5": {"name": "suicide_rate_male", "type": "FLOAT"},
            "SH.STA.SUIC.FE.P5": {"name": "suicide_rate_female", "type": "FLOAT"},
            "SH.STA.BASS.ZS": {"name": "sanitation_access", "type": "FLOAT"},
            "SH.STA.BASS.UR.ZS": {"name": "sanitation_access_urban", "type": "FLOAT"},
            "SH.STA.BASS.RU.ZS": {"name": "sanitation_access_rural", "type": "FLOAT"},
            "SH.H2O.BASW.ZS": {"name": "drinking_water_access", "type": "FLOAT"},
            "SH.H2O.BASW.UR.ZS": {
                "name": "drinking_water_access_urban",
                "type": "FLOAT",
            },
            "SH.H2O.BASW.RU.ZS": {
                "name": "drinking_water_access_rural",
                "type": "FLOAT",
            },
            "SH.STA.ODFC.ZS": {"name": "open_defecation_practice", "type": "FLOAT"},
            "SH.DYN.0509": {"name": "death_probability_0509", "type": "FLOAT"},
            "SH.DYN.1014": {"name": "death_probability_1014", "type": "FLOAT"},
            "SH.DYN.1519": {"name": "death_probability_1519", "type": "FLOAT"},
            "SH.DYN.2024": {"name": "death_probability_2024", "type": "FLOAT"},
            "SH.SGR.CRSK.ZS": {
                "name": "catastrophic_health_expenditure_risk",
                "type": "FLOAT",
            },
        },
        "rules": [],
    },
    "Population": {
        "attributes": {
            "Primary Key": {
                "name": "population_key",
                "type": "SERIAL PRIMARY KEY",
            },
            "SP.POP.TOTL.MA.ZS": {"name": "p_male", "type": "FLOAT"},
            "SP.POP.TOTL.FE.ZS": {"name": "p_female", "type": "FLOAT"},
            "SP.URB.TOTL.IN.ZS": {"name": "p_urban", "type": "FLOAT"},
            "SP.URB.TOTL.IN.ZS": {"name": "p_rural", "type": "FLOAT"},
            "SP.URB.GROW": {"name": "p_growth_urban", "type": "FLOAT"},
            "SP.RUR.TOTL.ZG": {"name": "p_growth_rural", "type": "FLOAT"},
            "SP.POP.0004.MA.5Y": {"name": "p_0004_male", "type": "FLOAT"},
            "SP.POP.0004.FE.5Y": {"name": "p_0004_female", "type": "FLOAT"},
            "SP.POP.0509.MA.5Y": {"name": "p_0509_male", "type": "FLOAT"},
            "SP.POP.0509.FE.5Y": {"name": "p_0509_female", "type": "FLOAT"},
            "SP.POP.1014.MA.5Y": {"name": "p_1014_male", "type": "FLOAT"},
            "SP.POP.1014.FE.5Y": {"name": "p_1014_female", "type": "FLOAT"},
            "SP.POP.1519.MA.5Y": {"name": "p_1519_male", "type": "FLOAT"},
            "SP.POP.1519.FE.5Y": {"name": "p_1519_female", "type": "FLOAT"},
            "SP.POP.2024.MA.5Y": {"name": "p_2024_male", "type": "FLOAT"},
            "SP.POP.2024.FE.5Y": {"name": "p_2024_female", "type": "FLOAT"},
            "SP.POP.2529.MA.5Y": {"name": "p_2529_male", "type": "FLOAT"},
            "SP.POP.2529.FE.5Y": {"name": "p_2529_female", "type": "FLOAT"},
            "SP.POP.3034.MA.5Y": {"name": "p_3034_male", "type": "FLOAT"},
            "SP.POP.3034.FE.5Y": {"name": "p_3034_female", "type": "FLOAT"},
            "SP.POP.3539.MA.5Y": {"name": "p_3539_male", "type": "FLOAT"},
            "SP.POP.3539.FE.5Y": {"name": "p_3539_female", "type": "FLOAT"},
            "SP.POP.4044.MA.5Y": {"name": "p_4044_male", "type": "FLOAT"},
            "SP.POP.4044.FE.5Y": {"name": "p_4044_female", "type": "FLOAT"},
            "SP.POP.4549.MA.5Y": {"name": "p_4549_male", "type": "FLOAT"},
            "SP.POP.4549.FE.5Y": {"name": "p_4549_female", "type": "FLOAT"},
            "SP.POP.5054.MA.5Y": {"name": "p_5054_male", "type": "FLOAT"},
            "SP.POP.5054.FE.5Y": {"name": "p_5054_female", "type": "FLOAT"},
            "SP.POP.5559.MA.5Y": {"name": "p_5559_male", "type": "FLOAT"},
            "SP.POP.5559.FE.5Y": {"name": "p_5559_female", "type": "FLOAT"},
            "SP.POP.6064.MA.5Y": {"name": "p_6064_male", "type": "FLOAT"},
            "SP.POP.6064.FE.5Y": {"name": "p_6064_female", "type": "FLOAT"},
            "SP.POP.6569.MA.5Y": {"name": "p_6569_male", "type": "FLOAT"},
            "SP.POP.6569.FE.5Y": {"name": "p_6569_female", "type": "FLOAT"},
            "SP.POP.7074.MA.5Y": {"name": "p_7074_male", "type": "FLOAT"},
            "SP.POP.7074.FE.5Y": {"name": "p_7074_female", "type": "FLOAT"},
            "SP.POP.7579.MA.5Y": {"name": "p_7579_male", "type": "FLOAT"},
            "SP.POP.7579.FE.5Y": {"name": "p_7579_female", "type": "FLOAT"},
            "SP.POP.80UP.MA.5Y": {"name": "p_80UP_male", "type": "FLOAT"},
            "SP.POP.80UP.FE.5Y": {"name": "p_80UP_female", "type": "FLOAT"},
        },
        "rules": "",
    },
    "WB_HNP": {
        "attributes": {
            "Primary Key": {"name": "id", "type": "SERIAL PRIMARY KEY"},
            "Month Key": {"name": "month_key", "type": "SERIAL"},
            "Country Key": {"name": "country_key", "type": "SERIAL"},
            "Population Key": {"name": "population_key", "type": "SERIAL"},
            "QualityOfLife Key": {"name": "qualityoflife_key", "type": "SERIAL"}
            # TODO: more PFKs & measures
        },
        "rules": [
            """
            CONSTRAINT fk_month 
                FOREIGN KEY(month_key) 
                    REFERENCES Month(month_key)
                        ON DELETE SET NULL
            """,
            """
            CONSTRAINT fk_country
                FOREIGN KEY(country_key)
                    REFERENCES Country(country_key)
	                    ON DELETE SET NULL
            """,
            """
            CONSTRAINT fk_population
                FOREIGN KEY(population_key) 
	                REFERENCES Population(population_key)
	                    ON DELETE SET NULL
            """,
            """
            CONSTRAINT fk_qualityoflife
                FOREIGN KEY(qualityoflife_key) 
	                REFERENCES QualityOfLife(qualityoflife_key)
	                    ON DELETE SET NULL
            """,
        ],
    },
}

atr: str
indicators = set()
for name, table in SCHEMA.items():
    for atr in table.get("attributes", {}).keys():
        if atr.count(".") >= 2:
            indicators.add(atr)

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
