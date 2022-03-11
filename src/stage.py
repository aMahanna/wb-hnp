import csv
import logging
from math import floor

from src import SCHEMA, dir_path, statsData


def write_month():
    logging.info("Executing: Write Month")
    MONTH = SCHEMA["Month"]
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
        fieldnames = list(MONTH["attributes"].keys())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        i = 1
        for year in range(2005, 2021):
            for j in range(0, 12):
                quarter = j // 3 + 1
                decade = floor(year / 10) * 10
                writer.writerow(
                    {
                        "month_key": i,
                        "code": j + 1,
                        "name": months[j],
                        "quarter": quarter,
                        "year": year,
                        "decade": decade,
                    }
                )

                i += 1


def write_country():
    logging.info("Executing: Write Country")
    COUNTRY_SCHEMA = SCHEMA["Country"]
    with open(f"{dir_path}/../csv/tables/Country.csv", "w", newline="") as outfile:
        fieldnames = (
            ["year"]
            + list(COUNTRY_SCHEMA["attributes"].keys())
            + list(ind["name"] for ind in COUNTRY_SCHEMA["indicators"].values())
        )
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        ################################ Load HNP_StatsCountry.csv ################################
        countryDict = {}
        with open(
            f"{dir_path}/../csv/attributes/HNP_StatsCountry.csv",
            newline="",
            mode="r",
            encoding="utf-8-sig",
        ) as StatsCountryCSV:
            readerStatsCountry = csv.DictReader(StatsCountryCSV)
            for i, row in enumerate(readerStatsCountry):
                # NOTE: NO DATA QUALITY ISSUES IN THIS FILE
                countryDict[row["Country Code"]] = {
                    "country_key": i + 1,
                    "name": row["Short Name"],
                    "code": row["Country Code"],
                    "region": row["Region"],
                    "currency": row["Currency Unit"],
                    "income_group": row["Income Group"],
                    "years": {},
                }

        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            country = countryDict[row["Country Code"]]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = COUNTRY_SCHEMA["indicators"][row["Indicator Code"]]["name"]
                ind_value = row[year]

                # TODO: HANDLE DATA QUALITY ISSUES HERE
                # if not ind_value ...

                country["years"][year][ind_name] = ind_value

        ################################ Write Country.csv ################################
        for i, country in enumerate(countryDict.values()):
            for j in range(2005, 2021):
                year = str(j)
                writer.writerow(
                    {
                        **{"year": year},
                        **{
                            atr: country[atr]
                            for atr in COUNTRY_SCHEMA["attributes"].keys()
                        },
                        **{
                            ind["name"]: country["years"][year][ind["name"]]
                            for ind in COUNTRY_SCHEMA["indicators"].values()
                        },
                    }
                )


# TODO: Data Staging, dump into CSV files
def main():
    write_month()
    write_country()
    logging.info("Success!")


if __name__ == "__main__":
    main()
