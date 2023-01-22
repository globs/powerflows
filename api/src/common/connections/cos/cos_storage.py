
class CosStorage(object):
    def __init__(self, secretname):
        self.secretname = secretname
        self.client = None
        self.utils = None
        

    def connect(self):
        #affect connexion to self.connexion
        pass

    def disconnect(self):
        #close self connexion
        pass    

