import os
import psycopg

filename = os.path.join(os.getcwd(),"sql/create_table_ghcnd_all.sql")

def run_sql_file (filename):
    # Connect to the database
    with psycopg.connect("dbname=weather user=matt") as conn:

        # Open a cursor to perform database operations
        with conn.cursor() as cur:

            # create ghcnd_all table
            with open(filename, "r") as sql_file:
                cur.execute(sql_file.read())

            # Make the changes to the database persistent
            conn.commit()

if __name__ == '__main__':
    run_sql_file(filename)