import csv
import logging
from src import conn, dir_path


def main():
    cursor = conn.cursor()
    cursor.execute("SELECT version()")
    data = cursor.fetchall()

    print("--------------")
    print(data)
    print("--------------")

    with open(f"{dir_path}/../csv/HNP_StatsData.csv", newline="") as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            # print(row['Country Code'])
            break


if __name__ == "__main__":
    main()
