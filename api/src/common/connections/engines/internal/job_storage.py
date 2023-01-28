
import logging
import pyarrow
import duckdb
import common.utils

#TODO store with job id and name key
class JobStorage(pbject):
    def __init__(self):
        self.internal_storage = {}


    def __del__(self):
        del sel.internal_storage


#internal storage types: ephemeral, localfs, pg, minio
    def storeRawData(self, job_id, asset_name, raw_data, storage_type):
        #TODO persist job storage to db and get id back
        #persist asset_id
        if storage_type == "job_storage":
            if job_name in self.internal_storage:
                self.internal_storage[job_name]['asset_name'] = raw_data
            else:
                 self.internal_storage[job_name] = {}
                 self.internal_storage[job_name]['asset_name'] = raw_data
        elif storage_type == "source_system":
            # instantiate connection from address
            # instantiate capability from address


    def getJobStorage(self, job_id, asset_name):
        res = None
        return res


    def persistJobStorageMetadata(self, )