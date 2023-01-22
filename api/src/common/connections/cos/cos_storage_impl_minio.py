import logging
import common.settings
import traceback
from minio import Minio
from common.connections.cos.cos_storage import CosStorage
from common.secrets.secret import SecretsManager
import json


class CosStorageMinio(CosStorage):
    def __init__(self, secretname):
        super().__init__(secretname)
        self.secrets_manager = SecretsManager()
        secret_json_str = self.secrets_manager.getSecretByNameJson(self.secretname)
        self.secret_json_dict = secret_json_str['secret']
        self.connect()

    def connect(self):
        # Create client with access and secret key.
        self.client = Minio(self.secret_json_dict['credentials']['url'],
        self.secret_json_dict['credentials']['accessKey'],
        self.secret_json_dict['credentials']['secretKey'],
        secure=False)
        buckets = self.client.list_buckets()
        for bucket in buckets:
            logging.info(f"Found Bucket: {bucket.name} created at: {bucket.creation_date}")

    def disconnect(self):
        #close self connexion
        pass    

    def downloadStringObjectData(self, params):
        # Get data of an object.
        bucket = params['bucketname']
        objectname = params['objectname']
        string = None
        response = None
        try:
            response = self.client.get_object(bucket, objectname)
            #beware of headers in csv file TODO create param
            string = response.read().decode('utf-8') #.split('\n', 1)[1]
            #logging.debug(string)
        finally:
            if response is None:
                logging.error(f'No file found for bucket:{bucket} objectname:{objectname}')
            else:
                response.close()
                response.release_conn()
            return string

    def uploadStringObjectData(self, upload_params):
        res = {
            "status":"OK",
            "config": upload_params
        }
        try:
            res['result'] = self.client.fput_object(upload_params['cos_bucket'], upload_params['cos_object_fullname'], upload_params['source_filepath'])
        finally:
            return res

    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, type, value, traceback):
        self.disconnect()