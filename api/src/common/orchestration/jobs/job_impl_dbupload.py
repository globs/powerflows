from common.orchestration.jobs.job import FlowJob
from common.connections.connection_factory import ConnectionFactory
import logging

"""
{
        "name" : "simple upload test from cos",
        "type" : "uploadfromcos",
        "cos_secret" : 'DefaultMinio',
        "db_secret": 'DefaultPG',
        "udpatemode" : "append", # append/replace/create,
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


class JobDBUpload(FlowJob):
    def __init__(self, config):
        super().__init__(config)
        self.cos_conn = ConnectionFactory(self.config_dict['cos_secret']).getConnectionImplFromSecret()
        self.db_conn = ConnectionFactory(self.config_dict['db_secret']).getConnectionImplFromSecret()

    def rolloutJob(self):
        jobconfig = self.config_dict
        run_res = []
        res_tmp = {
            "jobname" : jobconfig['name'],
        }
        logging.info(f"""
        job target db type {jobconfig['type']}
        bucket_name {jobconfig['config']['bucket_name']}
        object_name {jobconfig['config']['object_name']}
        schema {jobconfig['config']['schema']}
        table {jobconfig['config']['table']}
        colsep {jobconfig['config']['colsep']}
        """)
        try:
            if jobconfig['udpatemode'] == 'create':
                self.db_conn.dropTable(jobconfig['config']['schema'], jobconfig['config']['table'])
                self.db_conn.executeQuery(jobconfig['config']['table_ddl'])
            elif jobconfig['udpatemode'] == 'replace':
                self.db_conn.executeQuery(f"DELETE FROM {jobconfig['config']['schema']}.{jobconfig['config']['table']}")
            for objname in jobconfig['config']['object_name'].split(','):
                upload_settings = { "bucketname" : jobconfig['config']['bucket_name'],
                "objectname"  : objname,
                "schema" : jobconfig['config']['schema'],
                "table" : jobconfig['config']['table'],
                "colsep" : jobconfig['config']['colsep'] }
                self.db_conn.uploadFromCosStorage(upload_settings, self.cos_conn)
        except Exception as e:
            logging.error(f'something went wrong on job')
            res_tmp['error_message'] = f"something went wrong on job {e}"
            res_tmp['job status'] = f'ko'
            run_res.append(res_tmp)
            raise Exception(f'Error during Job {jobconfig} rollout')
        res_tmp['job status'] = f'ok'
        run_res.append(res_tmp)
        return run_res
