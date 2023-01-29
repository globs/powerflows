import logging
import common.settings

#TODO automatically add default values from connections.yaml
#TODO add mandatory/optional checks, 
#TODO add param types / vs referential checks
def capability_configurator(func):
    def with_capability_(*args,**kwargs):
        # pre function call instructions
        logging.info(f"ARGS: {args}")
        config_map = common.utils.getMapFromArray(args[1])
        placeholder_res_dict = {
            'statut' : 'Successful',
            'call_result': None
        }
        try:
            rv = func(config_map, *args,**kwargs)
        except Exception as e:
            logging.error(f'Error while decorating capability: {e}')
            raise
        else:
            logging.debug('Capability executed successfully')
        finally:
            logging.debug(f'Call return: {rv}')
        return rv
    return with_capability_