#from common.orchestration.jobs.job import FlowJob
#from common.orchestration.jobs.job_impl_dbupload import JobDBUpload
from common.secrets.secret import SecretsManager
from common.connections.connection_factory import ConnectionFactory
import logging
import common.utils
import importlib
import traceback 





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
        res = {
            "global_status":"Successful",
            "details":""
        }
        logging.info(f"""Starting Job execution: 
        Connections: {self.job_connections_config}
        Operations; {self.job_operations_config}
        """)
        results = []
        try:
            for connection in self.job_connections_config:
                logging.info(f'instanciation connection {connection} ')
            for operation in self.job_operations_config:
                logging.info(f'instanciation operation {operation} ')
                logging.info(f"instantiating connexion {operation['capability']['from_connection']}")
                connection_engine = ConnectionFactory(operation['capability']['from_connection']).getConnectionImplFromSecret()
                logging.info(f"Calling operation {operation['capability']['name']}")
                method_ = getattr(connection_engine, operation['capability']['name'])
                self.validate_job_config(operation['capability']['config'])
                results.append(method_(operation['capability']['config']))
        except Exception as e:
            logging.error(f"Error while executing job configuration: ${e}")
            traceback.print_exc()
            res['status'] = 'Failed'
            res['details'] = str(e)
        finally:
            res['job_results'] = results
            #TODO continue jobs if not mandatory execution
            return res

    #TODO Check config with config file conf/api/connections.yaml (parameters part) implement in common.utils if needed
    #TODO Raise exception if error 
    def validate_job_config(self, job_config_json):
        pass