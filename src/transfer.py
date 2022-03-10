##############################
# NOTE: CODE IS DECOMMISSIONED
##############################

import logging

from src import conn, dir_path, schema


def main():
    cursor = conn.cursor()

    # TODO: Grant bigboss the role of `pg_read_server_files`
    for name, table in schema.items():
        logging.info(f"Executing: COPY {name}")
        cursor.execute(
            f"""
            COPY {name}({', '.join(table['attributes'].keys())})
            FROM '{dir_path}/.../csv/tables/{name}.csv'
            DELIMITER ','
            CSV HEADER;
            """
        )

    conn.commit()
    conn.close()


if __name__ == "__main__":
    main()
