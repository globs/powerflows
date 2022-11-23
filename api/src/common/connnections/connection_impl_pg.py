import logging
from common.db.postgres.dbpooledconnector import DBPooledConnector
import psycopg2
import pandas as pd
import common.settings
import common.utils
import io
import os
import json
import traceback
import asyncio
from common.connections import Connection
from common.cos.cos_storage import CosStorage

class ConnexionPG():

    def __init__(self, credentials):
        super().__init__(credentials)
        

    def connect(self):
        #affect connexion to self.connexion
        self.connexion = DBPooledConnector().pg_pool.getconn()


    def disconnect(self):
        #close self connexion
        DBPooledConnector().pg_pool.putconn(self.connexion)    

    def executeQuery(self, query):
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

    def uploadFromCosStorage(self, upload_params):
        #bucketname, objectname, pg_schema, pg_table, csv_colsep
        self.utils.insertRawCSVToDB(str( upload_params['cos_storage'].downloadStringObjectData(upload_params['bucketname'], upload_params['objectname'])),
        upload_params['pg_schema'],
        upload_params['pg_table'],
        separator=upload_params['colsep'], 
        columns=None,
        truncate=True)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, type, value, traceback):
        self.disconnect()