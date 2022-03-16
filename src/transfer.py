import logging

import pandas as pd

from src import SCHEMA, conn, dir_path


def main():
    cursor = conn.cursor()

    for name in SCHEMA.keys():
        logging.info(f"Executing: COPY {name}")

        data = pd.read_csv(f"{dir_path}/../csv/tables/stage/{name}.csv")
        if name not in ["Month", "WB_HNP"]:
            data.drop("year_key", inplace=True, axis=1)

            if name != "Country":
                data.drop("country_key", inplace=True, axis=1)

        data.to_csv(f"{dir_path}/../csv/tables/transfer/{name}.csv", index=False)
        with open(f"{dir_path}/../csv/tables/transfer/{name}.csv", "r") as f:
            next(f)
            cursor.copy_from(f, name.lower(), sep=",", null="")

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
