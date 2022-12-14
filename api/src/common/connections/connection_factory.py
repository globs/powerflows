from common.db.dbconnection_impl_pg import DBConnexionPG
from common.cos.cos_storage_impl_minio import CosStorageMinio
from common.secrets.secret import SecretsManager
import json
import logging


class ConnectionFactory(object):
    def __init__(self, secretname):
        logging.info('Connection Factory instantiating...')
        self.secrets_manager = SecretsManager()
        self.secretname = secretname
        
        

    def getConnectionImplFromSecret(self):
        logging.info(f'Checking for secret metadata {self.secretname}')
        secret_json_dict = self.secrets_manager.getSecretByNameJson(self.secretname)
        if secret_json_dict['size'] > 0:
            logging.info(f'Found: {secret_json_dict}')
            if secret_json_dict['secret']['type'] == 'pg':
                logging.info('PostGreSQL Database secret detected')
                return DBConnexionPG(self.secretname)
            elif secret_json_dict['secret']['type'] == 'minio':
                logging.info('Minio COS secret detected')
                return CosStorageMinio(secret_json_dict['secret']['credentials'])
            else:
                raise ValueError(f"Unknow secret type {secret_json_dict['type']}, cannot instanciate corresponding connection type")
        else:
            raise Exception(f'Unknown secret type {secret_json_dict}, cannot instanciate class')
