
#https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-json/
import sqlite3
import json
import logging

class SecretsManager(object):
    def __init__(self ):
        dbfilepath = '/data/powerflows/secrets.db'
        conn = sqlite3.connect(dbfilepath)
        table_name = 'tsecrets'
        sql_check_if_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        dbfile = open(dbfilepath, 'r')
        if not dbfile.exists():
            raise Exception('No secret db available')
        else:
            cursor = conn.execute("SELECT id, name, address, salary from COMPANY")
            for row in cursor:
                logging.info( f"Secrets table found ")
            if row is None:
                 cursor = conn.execute("""
                 CREATE TABLE tsecrets
                 (
                    name text,
                    json_secret text,
                    create_date DEFAULT CURRENT_TIMESTAMP
                 )
                 """)
            json_secret_internal_pg = {"type": 78912,"name": "Jason Sweet","credentials": {"host": "XX", "port": "X", "user":"XXX", "password":"XXXXX", "db":"powerflows", "schema": "public"}}
            conn.execute(f"INSERT INTO tsecrets (NAME, SECRET_JSON) VALUES ('internal pg', {json.dumps(json_secret_internal_pg)})")
    


    def pullSecret(self, name):
        return None

    def pushSecret(self, name):
        return None
