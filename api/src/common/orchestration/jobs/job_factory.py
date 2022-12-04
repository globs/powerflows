from common.orchestration.jobs.job import FlowJob
from common.orchestration.jobs.job_impl_dbupload import JobDBUpload
from common.secrets.secret import SecretsManager
import logging





class FlowJobFactory(object):
    def __init__(self, json_settings):
        self.config = json_settings
        logging.info(f'FlowJobFactory call for settings: {json_settings}')

    
    def getJobImpl(self):
        if self.config['type'] == 'uploadfromcos':
            return JobDBUpload(self.config) #ConnectionPG(self.credentials)
        else:
            raise ValueError(costype)