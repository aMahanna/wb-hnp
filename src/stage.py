from collections import defaultdict
import csv
import json
import logging
from math import floor

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

countryNames = [
    "Canada",
    "US",
    "Mexico",
    "Iran",
    "China",
    "Lebanon",
    "Ukraine",
    "Vietnam",
    "India",
]


def int_s(p):
    if p == "":
        return None
    return float(p)


def createCountryToIndicators(indicators, rang=(1960, 2021)):
    country_to_indicators = {}

    with open(
        f"{dir_path}/../csv/attributes/HNP_StatsData.csv",
        newline="",
        mode="r",
        encoding="utf-8-sig",
    ) as StatsDataCSV:
        readerStatsData = csv.DictReader(StatsDataCSV)
        for row in readerStatsData:
            if row["Indicator Code"] in indicators:
                if row["Country Name"] in country_to_indicators:

                    country_to_indicators[row["Country Name"]][
                        row["Indicator Code"]
                    ] = [int_s(row[str(i)]) for i in range(rang[0], rang[1] + 1)]
                else:
                    country_to_indicators[row["Country Name"]] = {
                        row["Indicator Code"]: [
                            int_s(row[str(i)]) for i in range(rang[0], rang[1] + 1)
                        ]
                    }

    return country_to_indicators


def replaceNoneSame(arr):
    firstReal = 0
    for i, x in enumerate(arr):
        if x is not None:
            firstReal = i
            break
    to_ret = arr[::]
    for i in range(len(arr)):
        if arr[i] is None:
            if i == 0:
                to_ret[0] = arr[firstReal]
            else:
                to_ret[i] = to_ret[i - 1]
        else:
            to_ret[i] = arr[i]
    return to_ret


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
    with open(f"{dir_path}/../csv/tables/stage/Month.csv", "w", newline="") as outfile:
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
    COUNTRY_ATRS = SCHEMA["Country"]["attributes"]
    with open(
        f"{dir_path}/../csv/tables/stage/Country.csv", "w", newline=""
    ) as outfile:
        attributes = [atr["name"] for atr in COUNTRY_ATRS.values()]
        fieldnames = ["year_code"] + attributes
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
                    "name": row["Short Name"],
                    "code": row["Country Code"],
                    "region": row["Region"],
                    "currency": row["Currency Unit"],
                    "income_group": row["Income Group"],
                    "years": {},
                }

        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in COUNTRY_ATRS.keys():
                continue

            country = countryDict[row["Country Code"]]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = COUNTRY_ATRS[row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Country Data
                    if year == "2020":  # UNIQUE CASE: Use 2019 values
                        ind_value = row["2019"]

                country["years"][year][ind_name] = ind_value

        ################################ Write Country.csv ################################
        country_key = 1
        for code, country in countryDict.items():
            for j in range(2005, 2021):
                year = str(j)
                writer.writerow(
                    {
                        **{"year_code": year, "country_key": country_key},
                        **{
                            atr["name"]: country.get(atr["name"], None)
                            or country["years"][year][atr["name"]]
                            for atr in COUNTRY_ATRS.values()
                            if atr["name"] != "country_key"
                        },
                    }
                )

                country_key += 1


def write_population():
    logging.info("Executing: Write Population")
    POP_ATRS = SCHEMA["Population"]["attributes"]

    with open(
        f"{dir_path}/../csv/tables/stage/Population.csv", "w", newline=""
    ) as outfile:
        attributes = [atr["name"] for atr in POP_ATRS.values()]
        fieldnames = ["year_code", "country_code"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}
        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in POP_ATRS.keys():
                continue

            country = countryDict[row["Country Code"]]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = POP_ATRS[row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Population Data
                    # Should not happen
                    raise Exception(f"{ind_name} ({code}, {year})")

                country["years"][year][ind_name] = ind_value

        ################################ Write Population.csv ################################
        population_key = 1
        for code, country in countryDict.items():
            for j in range(2005, 2021):
                year = str(j)
                year_code = COUNTRY_MAP[code]
                writer.writerow(
                    {
                        **{
                            "year_code": year,
                            "country_code": year_code,
                            "population_key": population_key,
                        },
                        **{
                            atr["name"]: country["years"][year][atr["name"]]
                            for atr in POP_ATRS.values()
                            if atr["name"] != "population_key"
                        },
                    }
                )

                population_key += 1


def write_qualityoflife():
    logging.info("Executing: Write QualityOfLife")
    QOL_ATRS = SCHEMA["QualityOfLife"]["attributes"]

    with open(
        f"{dir_path}/../csv/tables/stage/QualityOfLife.csv", "w", newline=""
    ) as outfile:
        attributes = [atr["name"] for atr in QOL_ATRS.values()]
        fieldnames = ["year_code", "country_code"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}
        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in QOL_ATRS.keys():
                continue

            code = row["Country Code"]
            country = countryDict[code]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = QOL_ATRS[row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Population Data
                    # Unique Case: Missing values for Lebanon
                    if code == "LBN" and row["Indicator Code"] in [
                        "SH.STA.BASS.UR.ZS",
                        "SH.STA.BASS.RU.ZS",
                        "SH.H2O.BASW.UR.ZS",
                        "SH.H2O.BASW.RU.ZS",
                        "SH.SGR.CRSK.ZS",
                    ]:
                        ind_value = None  # Leave as null
                    elif year == "2020":  # Unique Class: Use 2019 values
                        ind_value = row["2019"]
                    else:
                        # Should not happen
                        raise Exception(f"{ind_name} ({code}, {year})")

                country["years"][year][ind_name] = ind_value

        ################################ Write QualityOfLife.csv ################################
        qualityoflife_key = 1
        for code, country in countryDict.items():
            for j in range(2005, 2021):
                year = str(j)
                country_code = COUNTRY_MAP[code]
                writer.writerow(
                    {
                        **{
                            "year_code": year,
                            "country_code": country_code,
                            "qualityoflife_key": qualityoflife_key,
                        },
                        **{
                            atr["name"]: country["years"][year][atr["name"]]
                            for atr in QOL_ATRS.values()
                            if atr["name"] != "qualityoflife_key"
                        },
                    }
                )

                qualityoflife_key += 1


def write_health():
    logging.info("Executing: Write Health")
    HEALTH_ATRS = SCHEMA["Health"]["attributes"]

    with open(f"{dir_path}/../csv/tables/stage/Health.csv", "w", newline="") as outfile:
        attributes = [atr["name"] for atr in HEALTH_ATRS.values()]
        fieldnames = ["year_code", "country_code"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}
        ################################ Load HNP_StatsData.csv ################################
        for row in statsData:
            if row["Indicator Code"] not in HEALTH_ATRS.keys():
                continue

            code = row["Country Code"]
            country = countryDict[code]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = HEALTH_ATRS[row["Indicator Code"]]["name"]
                ind_value = row[year]

                if not ind_value:  # Handle Missing Population Data
                    if year == "2020" and row["2019"]:
                        ind_value = row["2019"]  # Use 2019 for missing 2020 values
                    elif row["Indicator Code"] in [
                        "SH.MED.BEDS.ZS",
                        "SH.MED.PHYS.ZS",
                        "SH.MED.NUMW.P3",
                        "SH.IMM.IBCG",
                        "SH.STA.MMRT",
                    ]:
                        ind_value = (
                            None  # Nullify any non-2020 values for these indicators
                        )
                    else:
                        # Should not happen
                        raise Exception(f"{ind_name} ({code}, {year})")

                country["years"][year][ind_name] = ind_value

        ################################ Write Health.csv ################################
        health_key = 1
        for code, country in countryDict.items():
            for j in range(2005, 2021):
                year = str(j)
                country_code = COUNTRY_MAP[code]
                writer.writerow(
                    {
                        **{
                            "year_code": year,
                            "country_code": country_code,
                            "health_key": health_key,
                        },
                        **{
                            atr["name"]: country["years"][year][atr["name"]]
                            for atr in HEALTH_ATRS.values()
                            if atr["name"] != "health_key"
                        },
                    }
                )

                health_key += 1


def write_nutrition():
    logging.info("Executing: Write Nutrition")
    NUT_ATRS = SCHEMA["Nutrition"]["attributes"]

    with open(dir_path + "/../attributes.json", "r") as f:
        data = json.load(f)

    indicator_names = {}
    type_to_indicators = {}
    for indicatorType, t in data.items():
        type_to_indicators[indicatorType] = []
        for p, key in t.items():
            if "." in key:
                indicator_names[key] = p
                type_to_indicators[indicatorType].append(key)
    indicators = type_to_indicators["Nutrition"]

    X = createCountryToIndicators(indicators, rang=(2005, 2020))
    for country, dic in X.items():
        new_dict = {}
        for indicator_name, arr in dic.items():
            new_dict[indicator_name] = replaceNoneSame(arr)
        X[country] = new_dict

    with open(
        f"{dir_path}/../csv/tables/stage/Nutrition.csv", "w", newline=""
    ) as outfile:
        attributes = [atr["name"] for atr in NUT_ATRS.values()]
        fieldnames = ["year_code", "country_code"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        ################################ Write Nutrition.csv ################################
        nutrition_key = 1
        indicator_country_to_real_country = {
            "United States": "US",
            "Iran, Islamic Rep.": "Iran",
        }
        for country, indicators_dict in X.items():
            for j in range(2005, 2021):
                year = str(j)
                if country not in countryNames:
                    country_code = countryNames.index(
                        indicator_country_to_real_country[country]
                    )
                else:
                    country_code = countryNames.index(country) + 1

                writer.writerow(
                    {
                        **{
                            "year_code": year,
                            "country_code": country_code,
                            "nutrition_key": nutrition_key,
                        },
                        **{
                            NUT_ATRS[atr]["name"]: arr[j - 2005]
                            for atr, arr in indicators_dict.items()
                        },
                    }
                )

                nutrition_key += 1


def write_education():
    logging.info("Executing: Write Education")
    EDUCATION_ATRS = SCHEMA["Education"]["attributes"]

    with open(
        f"{dir_path}/../csv/tables/stage/Education.csv", "w", newline=""
    ) as outfile:
        attributes = [atr["name"] for atr in EDUCATION_ATRS.values()]
        fieldnames = ["year_code", "country_code"] + attributes
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        countryDict = {code: {"years": {}} for code in COUNTRY_MAP.keys()}

        for row in statsData:
            if row["Indicator Code"] not in EDUCATION_ATRS.keys():
                continue

            code = row["Country Code"]
            country = countryDict[code]
            for i in range(2005, 2021):
                year = str(i)
                if year not in country["years"]:
                    country["years"][year] = {}

                ind_name = EDUCATION_ATRS[row["Indicator Code"]]["name"]
                ind_value = row[year]

                country["years"][year][ind_name] = ind_value

        education_key = 1
        for code, country in countryDict.items():
            for j in range(2005, 2021):
                year = str(j)
                country_code = COUNTRY_MAP[code]
                writer.writerow(
                    {
                        **{
                            "year_code": year,
                            "country_code": country_code,
                            "education_key": education_key,
                        },
                        **{
                            atr["name"]: country["years"][year][atr["name"]]
                            for atr in EDUCATION_ATRS.values()
                            if atr["name"] != "education_key"
                        },
                    }
                )

                education_key += 1


def write_event():
    def calculate_date_key(date):
        MONTHS_MAP = {
            "January": 1,
            "February": 2,
            "March": 3,
            "April": 4,
            "May": 5,
            "June": 6,
            "July": 7,
            "August": 8,
            "September": 9,
            "October": 10,
            "November": 11,
            "December": 12,
        }
        date = date.split(" ")
        month, year = date[0], date[2]
        month_key = ((int(year) - 2005) * 12) + (MONTHS_MAP[month])
        return max(min(month_key, 192), 1)

    COUNTRY_ABR_MAP = {
        "Canada": "CAN",
        "US": "USA",
        "Mexico": "MEX",
        "Iran": "IRN",
        "China": "CHN",
        "Lebanon": "LBN",
        "Ukraine": "UKR",
        "Vietnam": "VNM",
        "India": "IND",
    }

    logging.info("Executing: Write Event")
    EVENT_ATRS = SCHEMA["Event"]["attributes"]

    with open(f"{dir_path}/../csv/tables/stage/Event.csv", "w", newline="") as outfile:
        attributes = [atr["name"] for atr in EVENT_ATRS.values()]
        writer = csv.DictWriter(outfile, fieldnames=attributes)
        writer.writeheader()

        with open(
            f"{dir_path}/../csv/attributes/country_event_data.csv",
            newline="",
            mode="r",
            encoding="utf-8-sig",
        ) as eventDataCSV:
            readerEventData = csv.DictReader(eventDataCSV)

            event_key = 1
            for row in readerEventData:
                writer.writerow(
                    {
                        "event_key": event_key,
                        "country_code": COUNTRY_MAP[COUNTRY_ABR_MAP[row["Country"]]],
                        "name": row["Name"],
                        "start_date": calculate_date_key(row["Start Date"]),
                        "end_date": calculate_date_key(row["End Date"]),
                        "deaths": row["Deaths"],
                    }
                )

                event_key += 1


def write_fact_table():
    logging.info("Executing: Write Fact Table")
    FACT_SCHEMA = SCHEMA["WB_HNP"]["attributes"]

    EVENT_DICT = {
        code: {month_key: set() for month_key in range(1, 193)}
        for code in COUNTRY_MAP.values()
    }
    with open(f"{dir_path}/../csv/tables/stage/Event.csv") as EventCSV:
        reader = csv.DictReader(EventCSV)
        for row in reader:
            for i in range(int(row["start_date"]), int(row["end_date"]) + 1):
                EVENT_DICT[int(row["country_code"])][i].add(int(row["event_key"]))

    with open(f"{dir_path}/../csv/tables/stage/WB_HNP.csv", "w", newline="") as outfile:
        fieldnames = [atr["name"] for atr in FACT_SCHEMA.values()]
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()

        fact_key = 1
        dimension_key = 1
        for country_code in range(1, 10):
            month_key = 1
            for _ in range(2005, 2021):
                for _ in range(1, 13):
                    if EVENT_DICT[country_code][month_key]:
                        for event_key in EVENT_DICT[country_code][month_key]:
                            writer.writerow(
                                {
                                    "fact_key": fact_key,
                                    "month_key": dimension_key,
                                    "country_key": dimension_key,
                                    "education_key": dimension_key,
                                    "health_key": dimension_key,
                                    "nutrition_key": dimension_key,
                                    "population_key": dimension_key,
                                    "qualityoflife_key": dimension_key,
                                    "event_key": event_key,
                                }
                            )

                            fact_key += 1
                    else:
                        writer.writerow(
                            {
                                "fact_key": fact_key,
                                "month_key": dimension_key,
                                "country_key": dimension_key,
                                "education_key": dimension_key,
                                "health_key": dimension_key,
                                "nutrition_key": dimension_key,
                                "population_key": dimension_key,
                                "qualityoflife_key": dimension_key,
                                "event_key": None,
                            }
                        )

                        fact_key += 1

                    month_key += 1

                dimension_key += 1


# TODO: Data Staging, dump into CSV files
def main():
    write_month()
    write_country()
    write_education()
    write_health()
    # write_nutrition()
    write_qualityoflife()
    write_population()
    write_event()
    write_fact_table()
    logging.info("Success!")


if __name__ == "__main__":
    main()
