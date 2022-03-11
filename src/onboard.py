import logging

from src import SCHEMA, conn


def main():
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {', '.join(SCHEMA.keys())};")

    table: dict
    for name, table in SCHEMA.items():
        attributes = table["attributes"].items()
        indicators = table.get("indicators", {}).values()

        atrs = ",".join([f"{atr} {type}" for atr, type in attributes])
        inds = ",".join([f"{ind['name']} {ind['type']}" for ind in indicators])
        rules = ",".join(table["rules"])

        parameters = f"{atrs}{',' + inds if inds else ''}{',' + rules if rules else ''}"
        query = f"CREATE TABLE {name}({parameters});"

        logging.info(f"Executing: CREATE TABLE {name}")
        cursor.execute(query)

    conn.commit()
    conn.close()
    logging.info("Success!")


if __name__ == "__main__":
    main()
