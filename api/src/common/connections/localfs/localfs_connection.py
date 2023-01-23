import logging
import common.utils
import requests
from common.decorators.capability_config import capability_configurator
from common.decorators.dbtrace import trace_to_db

#TODO use arrow for memory optimisation 

class LocalFSManager(object):
    def __init__(self):
        logging.info('Starting localfilesystem manager')


#Internal usage capabilities    
    def readFileToString(self, jobconfig):
        res = {
            "status":"Ok"
        }
        logging.info(f"reading file {jobconfig}")
        return res

    def writeStringToFile(self, jobconfig):
        res = {
            "status":"Ok"
        }
        logging.info(f"writing file with config {jobconfig}")
        return res

    @trace_to_db
    @capability_configurator
    def writeRestUrlToFile(config_map, self, config):
        res = {
            "status":"Ok"
        }
        try:
            source_url = config_map['source_url'] 
            target_path =  config_map['target_path']
            logging.info(f"""writing file with config 
            ** url: {source_url}
            ** target file path: {target_path}
            """)
            r = requests.get(source_url, allow_redirects=True)            
            open(target_path, 'wb+').write(r.content)
        except Exception as e:
            logging.error(f"""
            Error while importing url to file 
            ** With config: {jobconfig}
            ** Error: {e}
            """)
            res['status'] = "Failed"
        finally:
            return res

    @trace_to_db
    @capability_configurator
    def UploadToCOS(config_map, self, config):
        res = {
            "status":"Ok"
        }
        try:
            cos_engine_secret_name =  config_map['cos_connection'] #common.utils.getParameterValueFromJobConfig(jobconfig, 'cos_connection', 'value')
            logging.info(f"COS engine connection found in configuration: {cos_engine_secret_name}")
            cos_engine = common.utils.getEngineModuleFromSecretName(cos_engine_secret_name)
            logging.info(f"""
            Uploading to Cloud Object Storage 
            ** With parameters {params}
            """)
            res['result'] = cos_engine.uploadStringObjectData(config)
        except Exception as e:
            logging.error(f"Error while Uploading local file to Cloud Object Storage: {e}")
            logging.error(traceback.format_exc())
            res['status'] = "Error"
            res['error_message'] = str(e)
        finally:
            return res

    def DownloadFromCOS(self, jobconfig):
        res = {
            "status":"Ok"
        }
        logging.info(f"Downloading from cloud object storage with config {jobconfig}")
        return res

    def __del__(self):
        pass     

