

class DBConnection(object):
    def __init__(self, secretname):
        self.secretname = secretname
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

