import psycopg2
from psycopg2 import pool
import logging
import common.settings


def singleton(cls):    
    instance = [None]
    def wrapper(*args, **kwargs):
        if instance[0] is None:
            instance[0] = cls(*args, **kwargs)
        return instance[0]

    return wrapper

@singleton    
class DBPooledConnector():

    def __init__(self, dbcreds):
        logging.info("PostgreSQL connection pool initialized")
        self.pg_pool = psycopg2.pool.ThreadedConnectionPool(5, 50, 
                                    user = dbcreds['pg_user'],
                                    password = dbcreds['pg_pwd'],
                                    host = dbcreds['pg_host'],
                                    port = dbcreds['pg_port'],
                                    database = dbcreds['pg_db'])
        self.pg_pool.autocommit=True

