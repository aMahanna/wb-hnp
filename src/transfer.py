##############################
# NOTE: DECOMMISSIONED
##############################

import logging

from src import conn, dir_path, SCHEMA


def main():
    cursor = conn.cursor()

    # TODO: Grant bigboss the role of `pg_read_server_files`
    for name in SCHEMA.keys():
        logging.info(f"Executing: COPY {name}")
        cursor.execute(
            f"""
            COPY {name}
            FROM '{dir_path}/.../csv/tables/{name}.csv'
            DELIMITER ','
            CSV HEADER;
            """
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
