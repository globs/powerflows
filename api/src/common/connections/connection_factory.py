from common.connections.db.dbconnection_impl_pg import DBConnexionPG
from common.connections.cos.cos_storage_impl_minio import CosStorageMinio
from common.connections.localfs.localfs_connection import LocalFSManager
from common.secrets.secret import SecretsManager
import json
import logging


class ConnectionFactory(object):
    def __init__(self, secretname):
        logging.info(f'Connection Factory instantiating, searching for secret name: {secretname}')
        self.secrets_manager = SecretsManager()
        self.secretname = secretname
        secret_json_dict = self.secrets_manager.getSecretByNameJson(self.secretname)
        self.engine_type = secret_json_dict['secret']['group']
        self.connection_type = secret_json_dict['secret']['type']
        self.available_connections = [
            {
                "type":"pg",
                "module":"",
                "class":""
            },{
                "type":"minio",
                "module":"",
                "class":""
            }
        ]

        
        
#TODO use  self.available_connections for dynamic instantiation of connection implementations
    def getConnectionImplFromSecret(self):
        #TODO dynamic instantiation from conf/api/connections.yaml config file (module member)
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
            elif secret_json_dict['secret']['type'] == 'localfs':
                logging.info('Local Filesystem secret detected')
                return LocalFSManager()
            else:
                raise ValueError(f"Unknow secret type {secret_json_dict['type']}, cannot instanciate corresponding connection type")
        else:
            raise Exception(f'Unknown secret type {secret_json_dict}, cannot instanciate class')
