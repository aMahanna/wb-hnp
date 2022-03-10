import csv
import json
import logging
from math import floor

from src import dir_path, schema


def write_month():
    logging.info("Executing: Write Month")
    months = [
        "January",
        "February",
        "March",
        "April",
        "May",
        "June",
        "July",
        "August",
        "September",
        "October",
        "November",
        "December",
    ]
    with open(f"{dir_path}/../csv/tables/Month.csv", "w", newline="") as outfile:
        fieldnames = list(schema["Month"]["attributes"].keys())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        for year in range(2005, 2021):
            for i in range(0, 12):
                quarter = i // 3 + 1
                decade = floor(year / 10) * 10
                writer.writerow(
                    {
                        "code": i + 1,
                        "name": months[i],
                        "quarter": quarter,
                        "year": year,
                        "decade": decade,
                    }
                )


def write_country():
    logging.info("Executing: Write Country")
    with open(f"{dir_path}/../csv/tables/Country.csv", "w", newline="") as outfile:
        fieldnames = list(schema["Country"]["attributes"].keys())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        ################################ HNP_StatsCountry.csv ################################
        statsCountryDict = {}
        with open(
            f"{dir_path}/../csv/attributes/HNP_StatsCountry.csv",
            newline="",
            mode="r",
            encoding="utf-8-sig",
        ) as StatsCountryCSV:
            readerStatsCountry = csv.DictReader(StatsCountryCSV)
            for row in readerStatsCountry:
                statsCountryDict[row["Country Code"]] = {
                    "name": row["Short Name"],
                    "code": row["Country Code"],
                    "region": row["Region"],
                    "currency": row["Currency Unit"],
                    "income_group": row["Income Group"],
                }

        print(json.dumps(statsCountryDict, indent=2))
        ################################ HNP_StatsData.csv ################################
        # TODO


# TODO: Data Staging, dump into CSV files
def main():
    # write_month()
    # write_country()
    logging.info("Success!")


if __name__ == "__main__":
    main()
