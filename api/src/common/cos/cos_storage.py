from common.cos.cos_storage_impl_minio import CosStorageMinio

class CosStorage(object):
    def __init__(self, credentials):
        self.credentials = credentials
        self.client = None
        self.utils = None
        

    def connect(self):
        #affect connexion to self.connexion
        pass

    def disconnect(self):
        #close self connexion
        pass    


    def getStorageImplem(self, costype):
        if costype == self.credentials['costype']:
            return CosStorageMinio(self.credentials)
        else
            raise ValueError(costype)

