import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: This function can be used to drop existing staging, facts and dimensions tables tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.  

    Returns:
        None
    """      
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: This function can be used to create new staging, facts and dimensions tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.  

    Returns:
        None
    """  
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

        
def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print(" Files:{}".format(*config['S3'].values()))
    print("\n Connecting Redshift with Parameters: {} ...\n".format(*config['CLUSTER'].values()))
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(" Redshift Connected Successfully.\n")
    print(" STEP 1. Dropping Old Tables ...")
    drop_tables(cur, conn)
    print(" ****DONE !****\n")
    print(" STEP 2. Creating New Tables ...")
    create_tables(cur, conn)
    print(" ****DONE !****\n")
    conn.close()
    print(" Connection Closed.\n")
    print(" For Next STEP run the script ETL.py --> \n")

if __name__ == "__main__":
    main()