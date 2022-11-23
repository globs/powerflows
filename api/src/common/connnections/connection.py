from common.connections.connection_impl_pg import ConnectionPG



class Connection(object):
    def __init__(self, secretname):
        self.credentials = credentials
        self.connexion = None
        self.utils = None
        

    def connect(self):
        #affect connexion to self.connexion
        pass

    def disconnect(self):
        #close self connexion
        pass    

    def executeQuery(self, query):
        pass

    def offloadToCosStorage(self, offload_params):
        pass

    def uploadFromCosStorage(self, upload_params):
        pass

    def getConnectionImpl(self, dbtype):
        if dbtype == self.credentials['dbtype']:
            return ConnectionPG(self.credentials)
        else
            raise ValueError(costype)