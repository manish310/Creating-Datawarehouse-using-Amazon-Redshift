import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries,select_table_queries


def load_staging_tables(cur, conn):
    """
    Description: This function can be used to load data into staging tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None
    """  
    for i,query in enumerate(copy_table_queries):
        print("\n Loading Data into Staging Tables {} ... ".format(i+1))
        cur.execute(query)
        conn.commit()
        print(" ****DONE!**** ")


def insert_tables(cur, conn):
    """
    Description: This function can be used to insert data into the facts and dim tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object. 

    Returns:
        None
    """  
    for i,query in enumerate(insert_table_queries):
        print("\n {}. Inserting Data into {} Table ... ".format(i+1,query.split(" ")[2]))
        cur.execute(query)
        conn.commit()
        print(" ****DONE!**** ")
        
def select_tables(cur, conn):
    """
    Description: This function can be used to read number of records from the facts and dim tables.

    Arguments:
        cur: the cursor object. 
        conn: the connection object.  

    Returns:
        None
    """  
    for i,query in enumerate(select_table_queries):
        cur.execute(query)
        print("{}. Number of rows fetched in {} Table is:: {}".format(i+1,query.split("FROM",1)[1],cur.fetchone()[0]))
        conn.commit()

def main():
    print("////// WECOME TO THE ETL PROCESS PIPELINE //////")
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    print("\n Please wait... Connecting Redshift Cluster... ")
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    print(" Connected Successfully ! \n")
    #load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    print(":: Insertion Completed Successfully ::")
    print("\n******************************TABLE STATS*****************************\n")
    select_tables(cur, conn)
    print("\n**********************************************************************\n")

    conn.close()


if __name__ == "__main__":
    main()