import logging
import common.settings
import common.utils
import yaml


#TODO add mandatory/optional checks, 
#TODO add param types / vs referential checks
def trace_to_db(func):
    def with_function_(*args,**kwargs):
        # pre function call instructions
        try:
            pg_internal_engine = common.utils.getEngineModuleFromSecretName(common.settings.DEFAULT_PG_SECRET_NAME)
            rv = func(*args,**kwargs)
        except Exception as e:
            logging.error(f'Error while decorating capability: {e}')
            raise
        else:
            logging.debug('Capability executed successfully')
        finally:
            logging.info('********************************[Capability Started]********************************')
            #TODO Catch real function name
            #TODO use as an obseerver for job termination (socket.IO or websocket server)
            function_name = common.utils.clean_csv_value(func.__name__)
            result_call = common.utils.clean_csv_value(rv)
            #TODO TRuncate long json results in call_result member
            cleaned_result = result_call[1:100].replace('"',"").replace("'","")
            cleaned_result = result_call.replace('"',"").replace("'","")
            trace_query = f"INSERT INTO public.powerflows_traces (function_name, call_result) VALUES ('{function_name}', '{cleaned_result}')"
            trace_config_syaml= f"""---
            - parameter:
              name: sql_query
              value: >
                {trace_query}     
            - parameter:
              name: with_results
              value: false      
            """
            logging.debug(f"Tracing with str config {trace_config_syaml}")

            config = yaml.safe_load(trace_config_syaml)
            logging.debug(f"Tracing with config: {config}")
            pg_internal_engine.executeQueryInternal(config)
            logging.debug(f'Call return: {rv}')
            logging.info('********************************[Capability Ended]********************************')
        return rv
    return with_function_