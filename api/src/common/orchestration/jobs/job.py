from common.orchestration.jobs.job_impl_dbupload import JobDBUpload



class FlowJob(object):
    def __init__(self, json_settings):
        self.config = json_settings
        #allocate the right connection(s)
        pass

    def rolloutJob(self):
        pass

    def traceStart(self):
        pass

    def traceStop(self):
        pass

    def __enter__(self):
        self.traceStart()
        return self

    def __exit__(self, type, value, traceback):
        self.traceStop()

    def getJobImpl(self):
        if self.config['jobtype'] == 'dbupload':
            return None JobDBUpload(self.config)#ConnectionPG(self.credentials)
        else
            raise ValueError(costype)