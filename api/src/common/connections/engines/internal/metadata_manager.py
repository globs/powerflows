import logging
import yaml
import common.settings
import common.utils

class MetadataManager(object):
    def __init__(self):
        pass 
    
    def __del__(self):
        pass

    def createAsset(self, config):
        pass

    def getAssets(self, config):
        pass

    def getJobs(self, config):
        pass

    def createJob(self, config):
        pass

    def getTraces(self):
        pg_internal_engine = common.utils.getEngineModuleFromSecretName(common.settings.DEFAULT_PG_SECRET_NAME)
        trace_query = f"SELECT * FROM public.powerflows_traces ORDER BY TS DESC LIMIT 10"
        trace_config_syaml= f"""---
        - parameter:
            name: sql_query
            value: >
              {trace_query}     
        - parameter:
            name: with_results
            value: true      
        """
        config = yaml.safe_load(trace_config_syaml)
        logging.info(f"Tracing with config: {config}")
        res_call = pg_internal_engine.executeQueryInternal(config)
        return res_call['call_result']