import logging

from src import SCHEMA, conn


def main():
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {', '.join(SCHEMA.keys())};")

    table: dict
    for name, table in SCHEMA.items():
        attributes = table.get("attributes", {}).values()
        atrs = ",".join([f"{atr['name']} {atr['type']}" for atr in attributes])
        rules = ",".join(table["rules"])

        parameters = f"{atrs}{',' + rules if rules else ''}"
        query = f"CREATE TABLE {name}({parameters});"

        logging.info(f"Executing: CREATE TABLE {name}")
        cursor.execute(query)

    conn.commit()
    conn.close()
    logging.info("Success!")


if __name__ == "__main__":
    main()
