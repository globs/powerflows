import logging
from common.connections.db.postgres.dbpooledconnector import DBPooledConnector
import psycopg2
import pandas as pd
import common.settings
import common.utils
import io
import os
import json
import traceback
import asyncio
from common.connections.db.dbconnection import DBConnection
from common.connections.cos.cos_storage import CosStorage
from common.secrets.secret import SecretsManager


class DBConnexionPG(DBConnection):

    def __init__(self, secretname):
        super().__init__(secretname)
        self.secrets_manager = SecretsManager()
        secret_json_str = self.secrets_manager.getSecretByNameJson(self.secretname)
        self.secret_json_dict = secret_json_str['secret']
        self.connect()

    def __del__(self):
        self.disconnect()     
        

    def connect(self):
        #affect connexion to self.connexion
        self.connexion = DBPooledConnector(self.secret_json_dict['credentials']).pg_pool.getconn()


    def disconnect(self):
        #close self connexion
        DBPooledConnector(self.secret_json_dict['credentials']).pg_pool.putconn(self.connexion)    

    def executeQuery(self, sql, withResults=False):
        cnn = self.connexion
        res = []
        logging.info(sql)
        try:
            cursor = cnn.cursor()    
            cursor.execute(sql)
            if withResults:
                rows = cursor.fetchall()
                for row in rows:
                    logging.debug(row)
                    res.append(row)    
            else:
                cnn.commit()
        except: 
            logging.error(traceback.format_exc())    
        finally:
            cursor.close()            
        return res        


    def offloadToCosStorage(self, offload_params):
        pass

    def uploadFromCosStorage(self, upload_params, cos_connection):
        #bucketname, objectname, pg_schema, pg_table, csv_colsep
        logging.info('Downloading cos object locally')
        str_data = str(cos_connection.downloadStringObjectData(upload_params))
        logging.info(f'Downloaded object successfuly, starting upload to database : {str_data[1:10]}')
        self.insertRawCSVToDB(str_data,
        upload_params['schema'],
        upload_params['table'],
        separator=upload_params['colsep'], 
        columns=None,
        truncate=True)

    def dropTable(self, tabschema, tabname):
        logging.info(f'Dropping table {tabschema}.{tabname}')
        self.executeQuery(f'DROP TABLE IF EXISTS {tabschema}.{tabname} ;')

    def insertRawCSVToDB(self, rawdata, target_schema, target_table, separator, columns=None, truncate=False):
        if columns is None:
            columns = self.get_table_cols_list(target_table, "'id','ts'")
        if truncate:
            self.executeQuery(f"delete from {target_schema}.{target_table} a ")
        logging.info(f'separator: {separator}, columns {columns}')
        logging.debug(f'{rawdata}')
        strIO = io.StringIO()
        strIO.write(rawdata)
        strIO.seek(0)
        logging.info('Starting upload to db')
        self.insertStringIOToDB(strIO, target_schema, target_table, columns, separator)

    def insertStringIOToDB(self, stringio_data, target_schema, target_table, columns, separator):
        try:
            cnn = self.connexion
            cursor = cnn.cursor()
            cursor.execute(f"SET search_path TO {target_schema}")
            cursor.copy_from(stringio_data, target_table, sep=separator,columns=columns)
        except Exception as e:
            logging.error(f'Bulk insert in pg database error: {e}')
            raise Exception(e)
        finally:
            cursor.close()  
            cnn.commit()
            logging.info('String io upload to db done')
       #     DBPooledConnector().pg_pool.putconn(cnn)

    def get_table_cols_list(self, tabname, filteredcols=None):
        try:
            cnn = self.connexion
            res = []
            if filteredcols:
                additional_filter = f" AND column_name not in ({filteredcols}) order by ordinal_position"
            else:    
                additional_filter = 'order by ordinal_position'
            sql = f"SELECT column_name FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '{tabname}' {additional_filter} "
            cursor = cnn.cursor()
            cursor.execute(sql)
            columns = cursor.fetchall()
            for column in columns:
                res.append(column[0])
        except Exception as e:
            logging.error(f'error while getting columns list: {e}')
       # finally:
       #     DBPooledConnector().pg_pool.putconn(cnn)   
        return res


    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()