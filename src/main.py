import csv
import logging
from src import conn, dir_path


def main():
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    data = cursor.fetchall()

    for row in data:
        print(row)

    logging.info("SHOW TABLES; done")

    with open(f"{dir_path}/../csv/HNP_StatsData.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row["Country Code"])


if __name__ == "__main__":
    main()
