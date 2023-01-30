from common.orchestration.jobs.job import FlowJob
from common.orchestration.jobs.job_impl_dbupload import JobDBUpload
from common.secrets.secret import SecretsManager
import logging
import common.settings





class FlowJobFactoryDecorator(object):
    def __init__(self, str_config, type_str):
        self.config_str = str_config
        self.config_str_type = type_str
        if type_str == common.settings.JOB_CONFIG_FORMAT_YAML:
            self.config_json = common.utils.stryaml_to_json(str_config)
        elif  type_str == common.settings.JOB_CONFIG_FORMAT_JSON:
            self.config_json = common.utils.strjson_to_json(str_config)
        logging.info(f'FlowJobFactory call for settings: {self.config_json}')

    
    def getJobImpl(self):
        
        if self.config_json['kind'] == 'uploadfromcos':
            return JobDBUpload(self.config_json) #ConnectionPG(self.credentials)
        else:
            raise ValueError(costype)