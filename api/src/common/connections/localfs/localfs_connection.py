import logging
import common.utils
import requests

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

    #TODO Automate config parameters loading with conf/api/connections.yaml config file  
    def writeRestUrlToFile(self, jobconfig):
        res = {
            "status":"Ok"
        }
        try:
            source_url = common.utils.getParameterValueFromJobConfig(jobconfig, 'source_url', 'value')
            target_path = common.utils.getParameterValueFromJobConfig(jobconfig, 'target_path', 'value')
            #payload = common.utils.getParameterValueFromJobConfig(jobconfig, 'payload')
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

#End user capability
#TODO validate configuration
    def UploadToCOS(self, jobconfig):
        res = {
            "status":"Ok"
        }
        try:
            cos_engine_secret_name = common.utils.getParameterValueFromJobConfig(jobconfig, 'cos_connection', 'value')
            logging.info(f"COS engine connection found in configuration: {cos_engine_secret_name}")
            cos_engine = common.utils.getEngineModuleFromSecretName(cos_engine_secret_name)
            #TODO create a function to transform input settings to target settings
            params = {
                "cos_bucket":common.utils.getParameterValueFromJobConfig(jobconfig, 'cos_bucket', 'value'),
                "cos_object_fullname":common.utils.getParameterValueFromJobConfig(jobconfig, 'cos_object_fullname', 'value'),
                "source_filepath": common.utils.getParameterValueFromJobConfig(jobconfig, 'source_filepath', 'value')
            }
            logging.info(f"""
            Uploading to Cloud Object Storage 
            ** With parameters {params}
            """)
            res['result'] = cos_engine.uploadStringObjectData(params)
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

