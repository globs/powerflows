from common.orchestration.jobs.job import FlowJob
from common.orchestration.jobs.job_impl_dbupload import JobDBUpload
from common.secrets.secret import SecretsManager
import logging
import common.utils
import importlib


connection_modules= [
{
"type":"pg",
"module": "common."
},
{
"type":"minio",
"module": "common."
},
{
"type":"localfs",
"module": "common."
}
]


class FlowJobFactory(object):
    def __init__(self, yamlstr_settings):
        self.json_config = self.parseJsonSettings(yamlstr_settings)
        #logging.info(f'FlowJobFactory call for settings: {yamlstr_settings}')
        self.fullInit()
        logging.info(f"""Parsed JSON {
            self.json_config}
            ********************** Job Connections **********************
            {self.job_connections_config} 
            ********************** Job Operations **********************
            {self.job_operations_config}
        """)

    def fullInit(self):
        self.extractConnections()
        self.extractOperations()    


    def parseJsonSettings(self, stryaml_to_parse):
        return common.utils.stryaml_to_json(stryaml_to_parse)

    def extractConnections(self):
        self.job_connections_config = self.json_config['jobconfig']['connections']

    def extractOperations(self):
        self.job_operations_config = self.json_config['jobconfig']['operations']

    def executeJob(self):
        for connection in self.job_connections_config:
            logging.info(f'instanciation connection {connection} ')
        for operation in self.job_operations_config:
            logging.info(f'instanciation operation {operation} ')
        module = importlib.import_module('common.orchestration.jobs.job')
        class_ = getattr(module, 'FlowJob')
        dummy_settings = {}
        instance = class_(dummy_settings)


    def InstanciateJob(self):
        if self.config['type'] == 'uploadfromcos':
            return JobDBUpload(self.config) #ConnectionPG(self.credentials)
        else:
            raise ValueError(costype)