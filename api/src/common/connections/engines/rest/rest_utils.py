import requests
import logging
import json
import time
import traceback
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo
from common.decorators.capability_config import capability_configurator
from common.decorators.dbtrace import trace_to_db

throttling_calls_limit=1
throttling_time_limit=20

class RestUtils():

    def __init__(self, secretname, throttling_seconds=1, throttling_calls=20):
        self.secretname = secretname
        self.throttling_calls = throttling_calls
        self.throttling_seconds = throttling_seconds
        throttling_calls_limit = throttling_calls
        throttling_time_limit = throttling_seconds
        self.nb_api_calls = 0
    
    @sleep_and_retry
    @limits(calls=1200, period=60)
    @trace_to_db    
    @capability_configurator
    def performHttpRequest(config_map, self, config):
        res = {
            "capbility" : "performHttpRequest",
            "call_result" : None
        }
        for unit_config_map in config_map['requests_list']:
            res['call_result'] = self.InternalPerformHttpRequest(unit_config_map)
        return res
        



    def InternalPerformHttpRequest(self, config_map):
        res = {
            "status":"Successful",
            "result": None,
            "mime_type":None,
            "config": config_map
        }
        logging.info(f"""
        ** Starting http rest call with  
        ** Decorated config : {config_map}
        """)
        try:
            url = config_map['url']
            params = config_map['parameters']
            data =  config_map['data']
            headers = config_map['headers']
            logging.debug("Starting protected rest GET call - total #: {api_calls}".format(api_calls=self.nb_api_calls))
            logging.debug("execution get for url:{url}".format(url=url))
            self.nb_api_calls = self.nb_api_calls + 1 
            if config_map['operation'] == 'GET':
                response = requests.get(url, data=json.dumps(data), params=params, headers=headers, allow_redirects=True)
            else:
                response = requests.post(url, data=json.dumps(data), params=params, headers=headers, allow_redirects=True)
            response.raise_for_status()
            res['result'] = response.content
            if not response.status_code == 200:
                exit(500)
        except Exception as e:
            logging.error(f"""
            !!! Error while GET call: {str(e)}
            Traceback:
            {traceback.format_exc()}
            """)
            res['status'] = "Failed"
            res['error_message'] = traceback.format_exc()
        finally:
            logging.debug(f"Got {res}")
            return res  #json.loads(res.text)