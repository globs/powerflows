
import json
import logging

class FlowJob(object):
    def __init__(self, json_settings):
        self.config_dict = json_settings
        logging.info('Job interface')

    def rolloutJob(self):
        pass

    def traceStart(self):
        pass

    def traceStop(self):
        pass

    def __enter__(self):
        self.traceStart()
        return self

    def execute(self):
        logging.info('******************************************')

    def __exit__(self, type, value, traceback):
        self.traceStop()
