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
password = os.environ.get("PGSQL_PASS")

# See https://docs.microsoft.com/en-us/azure/postgresql/connect-python
conn = psycopg2.connect(f"host={host} user={user} dbname={dbname} password={password}")

logging.info(f"Connected to {host}: {dbname}")

dir_path = os.path.dirname(os.path.realpath(__file__))
