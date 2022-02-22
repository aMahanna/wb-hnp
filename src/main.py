import logging
from src import conn


def main():
    cursor = conn.cursor()
    cursor.execute("SHOW TABLES;")
    data = cursor.fetchall()

    for row in data:
        print(row)

    logging.info("done")


if __name__ == "__main__":
    main()
