import csv
import logging
from src import conn, dir_path


def main():
    cursor = conn.cursor()

    cursor.execute(
        f"""
        COPY month(name, quarter, year, decade)
        FROM {dir_path}/../csv/tables/month.csv
        DELIMITER ','
        CSV HEADER;
    """
    )

    cursor.execute(
        f"""
        COPY country(name, code, region, continent, currency, income_group, population, crude_birth_rate, crude_death_rate, labor_force, net_migration, life_expectancy, life_expectancy_male, life_expectancy_female)
        FROM {dir_path}/../csv/tables/month.csv
        DELIMITER ','
        CSV HEADER;
    """
    )

    # cursor.execute(f"""
    #     COPY WB_HNP(TODO)
    #     FROM {dir_path}/../csv/tables/month.csv
    #     DELIMITER ','
    #     CSV HEADER;
    # """)

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
