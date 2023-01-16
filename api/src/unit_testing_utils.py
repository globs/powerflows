import common.utils
import logging
import common.settings
import traceback

def main():
    try:
        common.settings.init_logging()
        logging.info('Staring commun.utils functions testing')
        test_json =  {'jobconfig': 
        {'name': 'job test1', 'kind': 'uploaddbfromcos', 'group': 'dbjobs', 'connections': ['minio default', 'postgresql default'],
         'operations':
          [
                {'operation': {'order': 1, 'name': 'op test1', 'type': 'load_from_cos', 'connection_main': ['postgresql'], 'config': {'bucket_name': 'xx', 'object_names': 'xx', 'schema_name': 'xx', 'table_name': 'xx', 'connection_cos': 'minio default'}}}, 
                {'operation': {'order': 2, 'name': 'op test2', 'type': 'load_from_cos', 'connections': ['postgresql default'], 'config': {'bucket_name': 'xx', 'object_names': 'yy', 'schema_name': 'xx', 'table_name': 'yy', 'connection_cos': 'minio defaultqa'}}}
           ]
        }}
        all_test_json_keys = common.utils.strjson_get_allkeys(test_json)
        logging.info(f'-----------------------> all parsed keys :')
        for pair in all_test_json_keys:
            logging.info(f'Key found: {pair}')
        logging.info('-----------------------> filtered keys with pattern connection')
        all_test_json_keys = common.utils.strjson_get_allkeys(test_json, 'cos')
        for pair in all_test_json_keys:
            logging.info(f'Key found: {pair}')

    except:
        logging.error(traceback.format_exc())

	


if __name__ == '__main__':
    main()
