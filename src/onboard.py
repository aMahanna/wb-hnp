import logging
from src import conn, dir_path

def main():
    cursor = conn.cursor()
    cursor.execute("DROP TABLE IF EXISTS month, country, wb_hnp;")

    month = f"""
        CREATE TABLE month (
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
    cursor.execute(month)


    country = f"""
        CREATE TABLE country(
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
    cursor.execute(country)

    wb_hnp = f"""
        CREATE TABLE wb_hnp (
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
    cursor.execute(wb_hnp)

    conn.commit()
    conn.close()
    logging.info("Success!")


if __name__ == "__main__":
    main()
