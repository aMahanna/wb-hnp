import logging

import pandas as pd

from src import SCHEMA, conn, dir_path


def main():
    cursor = conn.cursor()

    for name in SCHEMA.keys():
        logging.info(f"Executing: COPY {name}")

        data = pd.read_csv(f"{dir_path}/../csv/tables/stage/{name}.csv")

        if name == "WB_HNP": # Weird bug with Fact Table event_key 
            data['event_key'] = data['event_key'].astype('Int64')

        try:
            data.drop("year_code", inplace=True, axis=1)
            data.drop("country_code", inplace=True, axis=1)
        except:
            pass

        data.to_csv(f"{dir_path}/../csv/tables/transfer/{name}.csv", index=False)
        with open(f"{dir_path}/../csv/tables/transfer/{name}.csv", "r") as f:
            next(f)
            cursor.copy_from(f, name.lower(), sep=",", null="")

    logging.info("Success!")
    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
