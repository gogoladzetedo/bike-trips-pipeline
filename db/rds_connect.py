import psycopg2
import pandas as pd

config = {
    'dbname': 'postgres',
    'host': 'database-1.cgsoqcnxj6dd.eu-north-1.rds.amazonaws.com',
    'port': 5432,
    'user': 'postgres',
    'password': 'postgres'
}

def create_connection():
    conn = None
    try:
        conn = psycopg2.connect(dbname = config['dbname'],
                            host = config['host'],
                            port = config['port'],
                            user = config['user'],
                            password = config['password'])
        return conn
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def rds_exec_gracefully(_query):
    try:
        conn = create_connection()
        cur = conn.cursor()
        cur.execute(_query)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close()    

def rds_select_df(_query):
    try:
        conn = create_connection()
        return pd.read_sql_query(_query,con=conn)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if conn is not None:
            conn.close() 

