import os
import sqlite3
from pathlib import Path

def clear_db_if_exists(db_location,db_name):
    print("Going to clear database ,",db_name)
    os.remove(Path(db_location).joinpath(db_name))
    print("Cleared the database,",db_name)
    
def create_db(db_location,db_name):
    try:
        clear_db_if_exists(db_location,db_name)
        print("Creating the database,",db_name)
    except FileNotFoundError:
        print("Wrong path given to create the database,",db_location,db_name)
       
def create_connection(db_location,db_name):
    conn = sqlite3.connect(Path(db_location).joinpath(db_name))
    return conn
def close_connection(connection):
    connection.close()
    