import requests
import logging
import json
import time
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo

throttling_calls_limit=1
throttling_time_limit=20

class RestUtils():

    def __init__(self, throttling_seconds=1, throttling_calls=20):
        self.throttling_calls = throttling_calls
        self.throttling_seconds = throttling_seconds
        throttling_calls_limit = throttling_calls
        throttling_time_limit = throttling_seconds
        self.nb_api_calls = 0
    
    @sleep_and_retry
    @limits(calls=1200, period=60)
    def simple_get(self, url):
        res = None
        try:
            logging.debug("Starting protected rest GET call - total #: {api_calls}".format(api_calls=self.nb_api_calls))
            logging.debug("execution get for url:{url}".format(url=url))
            self.nb_api_calls = self.nb_api_calls + 1 
            res = requests.get(url)
            if res.status_code == 429:
                exit(429)
        except Exception as e:
            logging.error(f"Error while GET call: {str(e)}")
        finally:
            logging.debug(f"Got {res}")
            return res  #json.loads(res.text)