import  psycopg2

""" Connection String """
conn_dic = {
    "host"      : "postgres",
    "database"  : "postgres",
    "user"      : "postgres",
    "password"  : "postgres"
}

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**conn_dic)
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return 1 
    print("Connection successful")
    return conn

def execute_query(conn, command):
    """ Query Execution function """
    try:
        cur = conn.cursor()
        cur.execute(command)
        conn.commit()
        return cur
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1

def load_file(conn, file, table):
    try:
        cur = conn.cursor()
        cur.copy_from(file, table, sep=",")
        print("File Loaded Successfully...")
        conn.commit()
    except (Exception, psycopg2.DatabaseError) as error:
        print("Error: %s" % error)
        conn.rollback()
        return 1
    finally:
        cur.close()

def disconnect(conn):
    conn.close()