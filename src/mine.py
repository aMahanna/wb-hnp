from src import conn, dir_path
import pandas as pd

from sklearn.preprocessing import (
    MinMaxScaler,
    MaxAbsScaler,
    StandardScaler,
    RobustScaler,
)

SCHEMA = {
    "wb_hnp": [
        "gni",
        "life_satis",
    ],
    "country": ["life_expectancy", "crude_birth_rate", "crude_death_rate"],
    "health": [
        "domestic_health_expenditure",
        "immunization_hepb",
        "immunization_measles",
        "stillbirth_rate",
        "mort_infant",
        "mort_non_communicable_disease",
    ],
    "qualityoflife": [
        "unemployment_rate",
        "suicide_rate",
        "sanitation_access",
        "drinking_water_access",
        "catastrophic_health_expenditure_risk",
    ],
    "nutrition": [
        "hyperten",
        "undernour",
    ],
    "population": ["p_growth_urban", "p_growth_rural"],
}


def main():
    cursor = conn.cursor()
    query = f"""
        SELECT DISTINCT
            {','.join([table + '.' + attribute for table, attributes in SCHEMA.items() for attribute in attributes])}
        FROM 
            {','.join([table for table in SCHEMA.keys()])}
        WHERE
            {' AND '.join([f'wb_hnp.{table}_key = {table}.{table}_key' for table in SCHEMA.keys() if table != 'wb_hnp'])}
    """
    cursor.execute(query)
    data = cursor.fetchall()

    df = pd.DataFrame(
        list(map(list, data)),
        columns=[
            attribute for _, attributes in SCHEMA.items() for attribute in attributes
        ],
    )

    #############################################################################
    abs_scaler = MaxAbsScaler()
    df_abs = pd.DataFrame(abs_scaler.fit_transform(df), columns=df.columns)
    #############################################################################

    #############################################################################
    minmax_scaler = MinMaxScaler()
    df_minmax = pd.DataFrame(minmax_scaler.fit_transform(df), columns=df.columns)
    #############################################################################

    #############################################################################
    std_scaler = StandardScaler()
    df_std = pd.DataFrame(std_scaler.fit_transform(df), columns=df.columns)
    #############################################################################

    #############################################################################
    scaler = RobustScaler()
    df_robust = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
    #############################################################################

    print("--------------")
    print(df)
    print("--------------")


if __name__ == "__main__":
    main()
