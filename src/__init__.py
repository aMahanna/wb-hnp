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

schema = {
    "Month": {
        "primary_key": "month_key INT PRIMARY KEY",
        "attributes": {
            "name": "VARCHAR(9) NOT NULL",
            "quarter": "INT NOT NULL",
            "year": "INT NOT NULL",
            "decade": "INT NOT NULL",
        },
        "rules": "CHECK (quarter BETWEEN 1 AND 4)",
    },
    "Country": {
        "primary_key": "country_key INT PRIMARY KEY",
        "attributes": {
            "name": "VARCHAR(20) NOT NULL",
            "code": "VARCHAR(3) NOT NULL",
            "region": "VARCHAR(255) NOT NULL",
            "currency": "VARCHAR(255) NOT NULL",
            "income_group": "VARCHAR(255) NOT NULL",
            "population": "INT",
            "crude_birth_rate": "FLOAT",
            "crude_death_rate": "FLOAT",
            "labor_force": "INT",
            "net_migration": "INT",
            "life_expectancy": "FLOAT",
            "life_expectancy_male": "FLOAT",
            "life_expectancy_female": "FLOAT",
        },
        "rules": "",
    },
    # TODO
    # "Education": {
    #     "primary_key": "",
    #     "attributes": {},
    #     "rules": ""
    # },
    "WB_HNP": {
        "primary_key": "id INT PRIMARY KEY",
        "attributes": {
            "month_key": "INT",
            "country_key": "INT",
            # TODO: more PFKs & measures
        },
        "rules": """
            CONSTRAINT fk_month
                FOREIGN KEY(month_key) 
	                REFERENCES month(month_key)
	                ON DELETE SET NULL,
            CONSTRAINT fk_country
                FOREIGN KEY(country_key) 
	                REFERENCES country(country_key)
	                ON DELETE SET NULL
        """,
    },
}
