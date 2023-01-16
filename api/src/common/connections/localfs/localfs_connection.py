import logging


#TODO use arrow for memory optimisation 

class LocalFSManager(object):
    def __init__(self):
        logging.info('Starting localfilesystem manager')

    
    def readFileToString(self, jobconfig):
        logging.debug(f"reading file {jobconfig}")

    def writeStringToFile(self, jobconfig):
        logging.debug(f"writing file with config {jobconfig}")

    def writeUrlContentToFile(self, jobconfig):
        logging.debug(f"writing file with config {jobconfig}")

    def __del__(self):
        pass     