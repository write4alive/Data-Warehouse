import configparser
import psycopg2
from inf_as_code import creating_infrastructure_as_code as inf_as_code , cleanse_infrastructure as inf_clean
from sql_queries import copy_table_queries, insert_table_queries,create_table_queries


def create_tables(cur, conn):
    """

    Creates all tables inside cluster with using defined queries inside create_table_queries.

    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def load_staging_tables(cur, conn):
    """
    
    Loads data from s3 bucket from which defined inside dwh.cfg under s3 header.

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """

    inserting data to all tables in order.

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Here we are parsing parameters to our connection string from dwh.cfg file and trying to connect postgresql database inside created redshift cluster.

    Then we start the operation of 
    Creating required tables  , loading data from s3 bucket and filling them with insert operations.
    """

    # Creating infrastructure as code before starting etl operation.
    inf_as_code()

    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    create_tables(cur,conn)
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


    # uncomment to cleanse resources after we use.
    inf_clean()
    


if __name__ == "__main__":
    main()