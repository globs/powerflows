import logging, decimal, json, os
from logging.config import dictConfig
import yaml 
import sys

app_log_level=logging.INFO
logfilepath='../logs/cosTodb.log'
workingdir='/tmp/result_files/'

def init_logging():
	dictconfig = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "simple": {
            "format": "%(asctime)s - %(name)s - {%(filename)s:%(lineno)d}: %(levelname)s - %(message)s"
        }
    },
 
    "handlers": {
        "file_handler": {
            "class": "logging.FileHandler",
            "level": app_log_level,
            "formatter": "simple",
            "filename": logfilepath,
            "encoding": "utf8"},
        "console_handler": {
			'class': 'logging.StreamHandler',
              'formatter': 'simple',
              'level': app_log_level}
    },
 
    "root": {
        "level": app_log_level,
        "handlers": ["file_handler", "console_handler"]
    }
	}

	dictConfig(dictconfig)

connection_capabilties = None
app_conf = None

#TODO check internal services liveness here 
try:
    with open("../conf/api/connections.yaml", 'r') as file:
        connection_capabilties =  configuration = yaml.safe_load(file)
    with open( "../conf/api/app.yaml", 'r') as file:
        app_conf =  configuration = yaml.safe_load(file)
except Exception as e:
    logging.error(f"""
    Error while import application settings
    check that files ../conf/api/connections.yaml and ../conf/api/app.yaml exist
    error message {e}
    Exiting ...
    """)    
    sys.exit(-1)

#TODO init values from yaml previously loaded 
csv_separator=','

SQLLITE_DB_FILE='/tmp/result_files/secrets.db'

DEFAULT_PG_SECRET_NAME='PostGreSQL Default'
DEFAULT_MINIO_SECRET_NAME='Minio Default'

JOB_CONFIG_FORMAT_YAML='yaml'
JOB_CONFIG_FORMAT_JSON='json'

db_servers = """
[
    {
        "dbtype" : "pg",
        "creds" : {
        "pg_user":"vincent",
        "pg_password":"passtochange",
        "pg_host":"pg-datalab",
        "pg_port":"5432",
        "pg_app_db":"postgres"
       }
    },
    {
        "dbtype" : "db2",
        "creds" :{
        "db2_user":"db2inst1",
        "db2_password":"ChangeMe",
        "db2_host":"db2-datalab",
        "db2_port":"50000",
        "db2_app_db":"testdb"
       }
    },
    {
        "dbtype" : "mongo",
        "creds" :{
        "mongo_user":"vincent",
        "mongo_password":"gG9rV(@+LE5WyZmA",
        "mongo_host":"vps-98b06412.vps.ovh.net",
        "mongo_port":"5432",
        "mongo_app_db":"postgres"
       }
    }
    ]
"""



####old


def getdbcred(dbtype):
    # Transform json input to python objects
    input_dict = json.loads(db_servers)
    # Filter python objects with list comprehensions
    output_dict = [x for x in input_dict if x['dbtype'] == dbtype]
    # Transform python object back into json
    output_json = json.dumps(output_dict)
    return output_dict[0]


#mode create replace append
#todo add input filelist, truncate table, add http input support, csv profiling
db_transfers = """
[
   {
        "dbtype" : "pg",
        "source" :  "minio",
        "mode" : "create",
        "config": {
        "bucket_name":"main",
        "object_name":"lineitem.tbl.1",
        "schema":"public",
        "table":"lineitem",
        "csv_colsep":"|",
        "truncate": "true",
        "table_ddl" : "CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,\\n  L_PARTKEY     INTEGER NOT NULL,\\n  L_SUPPKEY     INTEGER NOT NULL,\\n  L_LINENUMBER  INTEGER NOT NULL,\\n  L_QUANTITY    DECIMAL(15,2) NOT NULL,\\n  L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,\\n  L_DISCOUNT    DECIMAL(15,2) NOT NULL,\\n  L_TAX         DECIMAL(15,2) NOT NULL,\\n  L_RETURNFLAG  CHAR(1) NOT NULL,\\n  L_LINESTATUS  CHAR(1) NOT NULL,\\n  L_SHIPDATE    DATE NOT NULL,\\n  L_COMMITDATE  DATE NOT NULL,\\n  L_RECEIPTDATE DATE NOT NULL,\\n  L_SHIPINSTRUCT CHAR(25) NOT NULL,\\n  L_SHIPMODE     CHAR(10) NOT NULL,\\n  L_COMMENT      VARCHAR(44) NOT null, C_FILLER CHAR(1) NULL);\\n"
       }
    },   
    {
        "dbtype" : "pg",
        "source" :  "minio",
        "mode" : "append",
        "config": {
        "bucket_name":"main",
        "object_name":"lineitem.tbl.2,lineitem.tbl.3,lineitem.tbl.4,lineitem.tbl.5,lineitem.tbl.6,lineitem.tbl.7,lineitem.tbl.8,lineitem.tbl.9,lineitem.tbl.10",
        "schema":"public",
        "table":"lineitem",
        "csv_colsep":"|",
        "table_ddl" : "CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,\\n  L_PARTKEY     INTEGER NOT NULL,\\n  L_SUPPKEY     INTEGER NOT NULL,\\n  L_LINENUMBER  INTEGER NOT NULL,\\n  L_QUANTITY    DECIMAL(15,2) NOT NULL,\\n  L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,\\n  L_DISCOUNT    DECIMAL(15,2) NOT NULL,\\n  L_TAX         DECIMAL(15,2) NOT NULL,\\n  L_RETURNFLAG  CHAR(1) NOT NULL,\\n  L_LINESTATUS  CHAR(1) NOT NULL,\\n  L_SHIPDATE    DATE NOT NULL,\\n  L_COMMITDATE  DATE NOT NULL,\\n  L_RECEIPTDATE DATE NOT NULL,\\n  L_SHIPINSTRUCT CHAR(25) NOT NULL,\\n  L_SHIPMODE     CHAR(10) NOT NULL,\\n  L_COMMENT      VARCHAR(44) NOT null, C_FILLER CHAR(1) NULL);\\n"
       }
    }

    ]
"""

#print(os.getenv('MINIO_CREDS_JSON'))

#minio_creds=json.loads(str(os.getenv('MINIO_CREDS_JSON'))) #{"url":"159.8.82.244:9000","accessKey":"VrpJ1vhxchPyVztU","secretKey":"pTk0BG4ihEY5NdFXbF6QWCnvMo6v9yMY","api":"s3v4","path":"auto"}
