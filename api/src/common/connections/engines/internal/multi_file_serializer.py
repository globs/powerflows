
import logging
import pyarrow
import duckdb
import common.utils
from copy import deepcopy
import pandas

config = {
    "json": {
        "csv": "a",
        "yaml": "a",
        "orc": "a",
        "parquet": "a",
        "arrow": "a",
        "pandas": "a"
    }
}

#TODO create functions for files, string, objectstores
#TODO add json to yaml etc 
class MultiSerializer(object):
    def __init__(self):
        self.internal_storage = {}

    def getArrowIntermediaryData(self):
        pass

    def __del__(self):
        pass


