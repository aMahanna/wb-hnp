import csv
import logging
from math import floor
from pickle import POP

from src import SCHEMA, dir_path, statsData

COUNTRY_MAP = {
    "CAN": 1,
    "USA": 2,
    "MEX": 3,
    "IRN": 4,
    "CHN": 5,
    "LBN": 6,
    "UKR": 7,
    "VNM": 8,
    "IND": 9,
}


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
        fieldnames = list(atr["name"] for atr in MONTH["attributes"].values())
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        month_key = 1
        for year in range(2005, 2021):
            for j in range(0, 12):
                quarter = j // 3 + 1
                decade = floor(year / 10) * 10
                writer.writerow(
                    {
                        "month_key": month_key,
                        "code": j + 1,
                        "name": months[j],
                        "quarter": quarter,
                        "year": year,
                        "decade": decade,
                    }
                )

                month_key += 1


def write_country():
    logging.info("Executing: Write Country")
    COUNTRY_SCHEMA = SCHEMA["Country"]
    with open(f"{dir_path}/../csv/tables/Country.csv", "w", newline="") as outfile:
        attributes = [atr["name"] for atr in COUNTRY_SCHEMA["attributes"].values()]
        fieldnames = ["month_key"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}
        ################################ Load HNP_StatsCountry.csv ################################
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
                    "country_key": COUNTRY_MAP[row["Country Code"]],
                    "name": row["Short Name"],
                    "code": row["Country Code"],
                    "region": row["Region"],
                    "currency": row["Currency Unit"],
                    "income_group": row["Income Group"],
                    "years": {},
                }

        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in COUNTRY_SCHEMA["attributes"].keys():
                continue

            country = countryDict[row["Country Code"]]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = COUNTRY_SCHEMA["attributes"][row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Country Data
                    if year == "2020":  # UNIQUE CASE: Use 2019 values
                        ind_value = row["2019"]

                country["years"][year][ind_name] = ind_value

        ################################ Write Country.csv ################################
        for code, country in countryDict.items():
            month_key = 1
            for j in range(2005, 2021):
                year = str(j)
                for _ in range(1, 13):
                    writer.writerow(
                        {
                            **{"month_key": month_key},
                            **{
                                atr["name"]: country.get(atr["name"], None)
                                or country["years"][year][atr["name"]]
                                for atr in COUNTRY_SCHEMA["attributes"].values()
                            },
                        }
                    )

                    month_key += 1


def write_population():
    logging.info("Executing: Write Population")
    POP_SCHEMA = SCHEMA["Population"]

    with open(f"{dir_path}/../csv/tables/Population.csv", "w", newline="") as outfile:
        attributes = [atr["name"] for atr in POP_SCHEMA["attributes"].values()]
        fieldnames = ["month_key", "country_key"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}
        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in POP_SCHEMA["attributes"].keys():
                continue

            country = countryDict[row["Country Code"]]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = POP_SCHEMA["attributes"][row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Population Data
                    raise Exception("balls")  # Should not happen

                country["years"][year][ind_name] = ind_value

        ################################ Write Population.csv ################################
        population_key = 1
        for code, country in countryDict.items():
            month_key = 1
            for j in range(2005, 2021):
                year = str(j)
                for _ in range(1, 13):
                    country_key = COUNTRY_MAP[code]
                    writer.writerow(
                        {
                            **{
                                "month_key": month_key,
                                "country_key": country_key,
                                "population_key": population_key,
                            },
                            **{
                                atr["name"]: country["years"][year][atr["name"]]
                                for atr in POP_SCHEMA["attributes"].values()
                                if atr["name"] != "population_key"
                            },
                        }
                    )

                    month_key += 1
                    population_key += 1


# TODO: Data Staging, dump into CSV files
def main():
    write_month()
    write_country()
    # write_education()
    # write_health()
    # write_nutrition()
    # write_qualityoflife()
    write_population()
    # write_event()
    # write_fact_table()
    logging.info("Success!")


if __name__ == "__main__":
    main()
