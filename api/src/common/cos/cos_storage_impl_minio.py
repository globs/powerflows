import logging
import common.settings
import traceback
from minio import Minio
import json


class CosStorageMinio(object):
    def __init__(self, credentials):
        super().__init__(credentials)

    def connect(self):
        # Create client with access and secret key.
        self.client = Minio(self.credentials['url'],
        self.credentials['accessKey'],
        self.credentials['secretKey'],
        secure=False)
        buckets = self.client.list_buckets()
        for bucket in buckets:
            print(bucket.name, bucket.creation_date)

    def disconnect(self):
        #close self connexion
        pass    

    def uploadStringObjectData(self, params):
        # Get data of an object.
        bucket = params['bucket']
        objectname = params['objectname']
        string = None
        response = None
        try:
            response = self.client.get_object(bucket, objectname)
            #beware of headers in csv file TODO create param
            string = response.read().decode('utf-8').split('\n', 1)[1]
            #logging.debug(string)
        finally:
            if response is None:
                logging.error(f'No file found for bucket:{bucket} objectname:{objectname}')
            else:
                response.close()
                response.release_conn()
            return string

    def downloadStringObjectData(self, upload_params):
        pass

    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, type, value, traceback):
        self.disconnect()