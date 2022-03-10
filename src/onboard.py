import logging
from src import conn, dir_path


def create_month():
    return f"""
        CREATE TABLE Month (
            month_key INT PRIMARY KEY, 
            name VARCHAR(9) NOT NULL,
            quarter INT NOT NULL,
            year INT NOT NULL,
            decade INT NOT NULL,
            CHECK (quarter BETWEEN 1 AND 4),
            CONSTRAINT fk_month
                FOREIGN KEY(month_key) 
	                REFERENCES month(month_key)
	                ON DELETE SET NULL
        );
    """


def create_country():
    return f"""
        CREATE TABLE Country(
            country_key INT PRIMARY KEY,
            name VARCHAR(20) NOT NULL,
            code VARCHAR(3) NOT NULL,
            region VARCHAR(255) NOT NULL,
            continent VARCHAR(255) NOT NULL,
            currency VARCHAR(255) NOT NULL,
            income_group VARCHAR(255) NOT NULL,
            population INT,
            crude_birth_rate FLOAT,
            crude_death_rate FLOAT,
            labor_force INT,
            net_migration INT,
            life_expectancy FLOAT,
            life_expectancy_male FLOAT,
            life_expectancy_female FLOAT
        );
    """


def create_fact_table():
    return f"""
        CREATE TABLE WB_HNP (
            fact_key INT PRIMARY KEY, 
            month_key INT,
            country_key INT,
            CONSTRAINT fk_month
                FOREIGN KEY(month_key) 
	                REFERENCES month(month_key)
	                ON DELETE SET NULL,
            CONSTRAINT fk_country
                FOREIGN KEY(country_key) 
	                REFERENCES country(country_key)
	                ON DELETE SET NULL
        );
    """


def main():
    cursor = conn.cursor()
    cursor.execute(
        "DROP TABLE IF EXISTS Month, Country, Education, Health, Nutrition, QualityOfLife, WB_HNP;"
    )

    cursor.execute(create_month())
    cursor.execute(create_country())
    # cursor.execute(create_education())
    # cursor.execute(create_health())
    # cursor.execute(create_nutrition())
    # cursor.execute(create_qualityoflife())
    cursor.execute(create_fact_table())

    conn.commit()
    conn.close()
    logging.info("Success!")


if __name__ == "__main__":
    main()
