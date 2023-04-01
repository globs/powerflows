import logging
import yaml
import common.settings
import common.utils

class MetadataManager(object):
    def __init__(self):
        self.pg_internal_engine = common.utils.getEngineModuleFromSecretName(common.settings.DEFAULT_PG_SECRET_NAME) 
    
    def __del__(self):
        pass

    def createAsset(self, config):
        pass

    def getAssets(self, config):
        pass

    def getJobs(self, config):
        pass


    def createAsset(self, config):
        sql_query = f"""INSERT INTO public.powerflows_assets 
              (asset_name,asset_type,asset_address, asset_json_def) 
              VALUES 
              ('{config['asset_name']}', '{config['asset_type']}', 'jobs.internal.{config['asset_name']}', '{config['asset_yaml']}')
        """
        config_map = {}
        config_map['sql_query'] = sql_query
        config_map['with_results'] = False
        logging.info(f"Creating asset with config: {config_map}")
        res_call = self.pg_internal_engine.executeQueryJsonConfig(config_map)
        return res_call['call_result']
        
    def getAssetConfig(self, config):
        asset_name = config['asset_name']
        asset_type = config['asset_type']
        #asset_yaml = config['asset_yaml']
        sql_query = f"SELECT asset_json_def FROM public.powerflows_assets a where a.asset_name='{asset_name}' and a.asset_type='{asset_type}'"
        asset_config_syaml= f"""---
        - parameter:
            name: sql_query
            value: >
              {sql_query}     
        - parameter:
            name: with_results
            value: true      
        """
        config_sql = yaml.safe_load(asset_config_syaml)
        logging.info(f"Searching Asset with with config: {config_sql}")
        res_call = self.pg_internal_engine.executeQueryInternal(config_sql)        
        logging.info(f"Found asset: {res_call}")
        if len(res_call['call_result']) > 0:
            logging.info(f"Res type {type(res_call['call_result'][0][0])} \n Content: {res_call['call_result'][0][0]}")
            return res_call['call_result'][0][0]          
        else:
            logging.info(f"Asset {asset_name} not found")
            return None


    def getTraces(self):
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
        res_call = self.pg_internal_engine.executeQueryInternal(config)
        return res_call['call_result']
    
    def getJobs(self, config):
        trace_query = f"SELECT * FROM public.powerflows_assets where  asset_name = '{config['job_name']}'"
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
        res_call = self.pg_internal_engine.executeQueryInternal(config)
        return res_call['call_result']        