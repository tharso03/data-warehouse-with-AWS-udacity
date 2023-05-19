import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into staging tables in Redshift.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing queries.
        conn (psycopg2.extensions.connection): Connection object for connecting to Redshift.

    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data into all tables in the database using the provided cursor and connection.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing SQL statements.
        conn (psycopg2.extensions.connection): Connection object for connecting to the database.

    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Main function that loads data from S3 into Redshift and inserts data into fact and dimension tables.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()