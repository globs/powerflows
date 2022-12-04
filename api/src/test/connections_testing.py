from common.secrets.secret import SecretsManager
from common.connections.connection_factory import ConnectionFactory
from common.orchestration.jobs.job_factory import FlowJobFactory
import logging
import json
import common.settings
import traceback

def main():
    try:
        common.settings.init_logging()
        pg_connection = ConnectionFactory('PostGreSQL Default').getConnectionImplFromSecret()
        logging.info('PostGRESQL Connection ---->[OK]')
        minio_connection = ConnectionFactory('Minio Default').getConnectionImplFromSecret()
        logging.info('Minio Connection ---->[OK]')
        UploadJobConfig = """
        {
                "name" : "simple upload test from cos",
                "type" : "uploadfromcos",
                "cos_secret" : "Minio Default",
                "db_secret": "PostGreSQL Default",
                "udpatemode" : "append",
                "config": 
                {
                "bucket_name":"main",
                "object_name":"csv/nation.tbl",
                "schema":"public",
                "table":"nation",
                "inputformat" : "csv",
                "colsep":"|",
                "table_ddl" : "CREATE TABLE LINEITEM ( L_ORDERKEY    INTEGER NOT NULL,  L_PARTKEY     INTEGER NOT NULL,  L_SUPPKEY     INTEGER NOT NULL,  L_LINENUMBER  INTEGER NOT NULL,  L_QUANTITY    DECIMAL(15,2) NOT NULL,  L_EXTENDEDPRICE  DECIMAL(15,2) NOT NULL,  L_DISCOUNT    DECIMAL(15,2) NOT NULL,  L_TAX         DECIMAL(15,2) NOT NULL,  L_RETURNFLAG  CHAR(1) NOT NULL,  L_LINESTATUS  CHAR(1) NOT NULL,  L_SHIPDATE    DATE NOT NULL,  L_COMMITDATE  DATE NOT NULL,  L_RECEIPTDATE DATE NOT NULL,  L_SHIPINSTRUCT CHAR(25) NOT NULL,  L_SHIPMODE     CHAR(10) NOT NULL,  L_COMMENT      VARCHAR(44) NOT null /*, C_FILLER CHAR(1) NULL */ );"
            }
        }
        """
        UploadFromCosTestJob = FlowJobFactory(json.loads(UploadJobConfig)).getJobImpl()
        UploadFromCosTestJob.rolloutJob()
        #secrets_manager.getAllSecrets()
    except:
        logging.error(traceback.format_exc())

	


if __name__ == '__main__':
    main()
