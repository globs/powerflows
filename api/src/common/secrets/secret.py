
#https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-json/
import sqlite3
import json
import os
import logging
from os.path import exists
import common.settings


class SecretsManager(object):
    def __init__(self ):
        logging.info('Starting secrets manager python backend')
        self.dbfilepath = common.settings.SQLLITE_DB_FILE
        self.conn = sqlite3.connect(self.dbfilepath)
        self.checkIfConfigExistsOrInit()
    


    def checkIfConfigExistsOrInit(self):
        table_name = 'tsecrets'
        sql_check_if_exists = f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}';"
        data = self.sqlite_execute_query(sql_check_if_exists, True)
        if data is None or len(data) == 0:
            logging.info('initializing sqlite db')
            self.sqlite_execute_query("""
            CREATE TABLE tsecrets
            (
                name text,
                json_secret text,
                create_date DEFAULT CURRENT_TIMESTAMP
            )
            """)
        else:
            logging.info('Secret sqlite table found, table creating skipped')

        tst_minio_secret = self.pullSecret(str(common.settings.DEFAULT_MINIO_SECRET_NAME))
        tst_pg_secret = self.pullSecret(common.settings.DEFAULT_PG_SECRET_NAME)
        logging.debug(f'#config results found for db: {tst_minio_secret}, cos: {tst_pg_secret}')
        if  tst_minio_secret['size'] == 0: 
            logging.debug(f'Adding internal minio cos credentials to secrets, res for debug : {tst_minio_secret}')
            json_secret_internal_minio =str(os.getenv('MINIO_CREDS_JSON')).replace('***NAME***', f"{common.settings.DEFAULT_MINIO_SECRET_NAME}")
            minio_secret = json.loads(json_secret_internal_minio)
            self.pushSecretJson(minio_secret)
        else:
            logging.debug('Minio secret already exists, init minio cos skipped')
        if  tst_pg_secret['size'] == 0: 
            logging.debug('Adding internal pg db credentials to secrets')
            pg_secret = {"group": "dbops", "type": "pg", "name": f"{common.settings.DEFAULT_PG_SECRET_NAME}", "credentials":  {"pg_host": f"{os.getenv('pg_host')}",  "pg_port": f"{os.getenv('pg_port')}",  "pg_user": f"{os.getenv('pg_user')}",  "pg_pwd": f"{os.getenv('pg_pwd')}",  "pg_db": f"{os.getenv('pg_db')}"} }
            self.pushSecretJson(pg_secret)
        else:
            logging.debug('internal pg db secret already exists, init skipped')
            

    def pullSecret(self, name):
        return self.getSecretByNameJson(name)

    def pushSecretJson(self, secret):
        logging.debug(f"Received json secret type {type(secret)}: {secret}")
        if type(secret) == dict:
            str_secret = json.dumps(secret)
            json_secret = secret
        elif type(secret) == str:
            str_secret = secret
            json_secret = json.loads(secret)
        else:
            raise Exception(f"secret python datatype not recognized: {type(secret)}")
        logging.debug(f"pushing secret to db: {str_secret}")
        self.sqlite_execute_query(f"INSERT INTO tsecrets (name, json_secret) VALUES ('{json_secret['name']}', '{str_secret}')")

    def getSecretByNameJson(self, name):
        sql_search = f"SELECT  json_secret FROM tsecrets a where a.name = '{name}';"
        logging.debug(f'Searching for secret *{name}* with query {sql_search}')
        query_data = self.sqlite_execute_query(sql_search, True)
        if  query_data['size'] > 0:
            res = { "secret" : json.loads(query_data['data'][0][0]) , "size" : query_data['size']}
        else:
            logging.error('!!! Secret not found')
            res = { "secret" : None , "size" : query_data['size']}
        return res

    def getAllSecrets(self):
        logging.debug('Fetching all secrets')
        sql_search = f"SELECT name, json_secret FROM tsecrets a;"
        res = self.sqlite_execute_query(sql_search, True)
        for row in res:
            logging.debug(f'Secret display: {row[0]}: {row[1]}')
         

    def sqlite_execute_query(self, sql_query, withresults=False, returntype='array'):
        logging.info(f'Executing SQL Query {sql_query}')
        data = self.conn.execute(sql_query)
        self.conn.commit()
        if withresults:
            res = data.fetchall()
            logging.debug(f'Query results : {res}')
        if withresults and returntype == 'array':
            return {"data": res, "size" : len(res)}
        elif withresults:
            return data
        else:
            return None