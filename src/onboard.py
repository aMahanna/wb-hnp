import logging

from src import conn, schema


def main():
    cursor = conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {', '.join(schema.keys())};")

    table: dict
    for name, table in schema.items():
        primary_key = table["primary_key"]
        attributes: dict = table["attributes"]
        rules: str = table["rules"]

        parameters = f"{''.join([f'{atrb} {type}, ' for atrb, type in attributes.items()])}{rules}"
        query = f"CREATE TABLE {name}({parameters.rstrip(', ')});"
        logging.info(f"Executing: CREATE TABLE {name}")
        cursor.execute(query)

    conn.commit()
    conn.close()
    logging.info("Success!")


if __name__ == "__main__":
    main()
