import csv

from src import conn, dir_path


def main():
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    version = cursor.fetchall()

    print("--------------")
    print(version)
    print("--------------")

    with open(
        f"{dir_path}/../csv/attributes/HNP_StatsCountry.csv",
        newline="",
        mode="r",
        encoding="utf-8-sig",
    ) as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            print(row["Country Code"])


if __name__ == "__main__":
    main()
