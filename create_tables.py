import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables in the database using the provided cursor and connection.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing SQL statements.
        conn (psycopg2.extensions.connection): Connection object for connecting to the database.

    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables in the database using the provided cursor and connection.

    Args:
        cur (psycopg2.extensions.cursor): Cursor object for executing SQL statements.
        conn (psycopg2.extensions.connection): Connection object for connecting to the database.

    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Connects to the Redshift cluster using the configuration details in `dwh.cfg`, drops any existing tables, and creates new tables.

    Args:
        None

    Returns:
        None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()