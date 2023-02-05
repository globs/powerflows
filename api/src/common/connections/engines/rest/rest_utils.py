import requests
import logging
import json
import pyarrow as pa
from pyarrow import json  as arrow_json
import duckdb
import time
import io
import traceback
from ratelimit import limits, sleep_and_retry
from backoff import on_exception, expo
from common.decorators.capability_config import capability_configurator
from common.decorators.dbtrace import trace_to_db
from common.connections.engines.internal.multi_file_serializer import MultiSerializer
import common.utils

throttling_calls_limit=1
throttling_time_limit=20

class RestUtils():

    def __init__(self, secretname, throttling_seconds=1, throttling_calls=20):
        self.secretname = secretname
        self.throttling_calls = throttling_calls
        self.throttling_seconds = throttling_seconds
        throttling_calls_limit = throttling_calls
        throttling_time_limit = throttling_seconds
        self.nb_api_calls = 0
        self.internal_dict = {}
    
    @sleep_and_retry
    @limits(calls=1200, period=60)
    @trace_to_db    
    @capability_configurator
    def performHttpRequest(config_map, self, config):
        res = {
            'status' : 'Successful',
            'call_result': []
        }
        for unit_config_map in config_map['requests_list']:
            res['call_result'].append(self.InternalPerformHttpRequest(unit_config_map))
        
        return res
        



    def InternalPerformHttpRequest(self, config_map):
        res = {
            "status":"Successful",
            "response": None,
            "mime_type":None,
            "config": config_map
        }
        logging.info(f"""
        ** Starting http rest call with  
        ** Decorated config : {config_map}
        """)
        try:
            url = config_map['url']
            params = config_map['parameters']
            data =  config_map['data']
            headers = config_map['headers']
            logging.debug("Starting protected rest GET call - total #: {api_calls}".format(api_calls=self.nb_api_calls))
            logging.debug("execution get for url:{url}".format(url=url))
            self.nb_api_calls = self.nb_api_calls + 1 
            if config_map['operation'] == 'GET':
                response = requests.get(url, data=json.dumps(data), params=params, headers=headers, allow_redirects=True)
            else:
                response = requests.post(url, data=json.dumps(data), params=params, headers=headers, allow_redirects=True)
            response.raise_for_status()
            logging.debug(f"Response contents {response.content}")
            res['response'] = response.content
            if 'jobstorage_persist_result' in config_map and config_map['jobstorage_persist_result']:
                self.persistResult(config_map, response)
                #FIXME put this snippet in common.utils

            if not response.status_code == 200:
                exit(500)
        except Exception as e:
            logging.error(f"""
            !!! Error while GET call: {str(e)}
            Traceback:
            {traceback.format_exc()}
            """)
            res['status'] = "Failed"
            res['error_message'] = traceback.format_exc()
        finally:
            logging.debug(f"Got {res}**")
            return res  #json.loads(res.text)



    def persistResult(self, config_map, response):
        asset_address_array = config_map['jobstorage_asset_address'].split('.')
        jobstorage_secret_name = asset_address_array[0]
        if jobstorage_secret_name == "internal":
            jobstorage_dict_entry = asset_address_array[1]
            jobstorage_object_name = asset_address_array[2]
            strIO = io.StringIO()
            strIO.write(response.text)
            strIO.seek(0)                
            self.internal_dict['jobstorage_dict_entry'] = {}                
            open(f"/tmp/result_files/{jobstorage_object_name}", 'wb+').write(response.content)
            if config_map['result_mime_type'] == 'application/json':
                self.internal_dict['jobstorage_dict_entry']['jobstorage_object_name'] = arrow_json.read_json(f"/tmp/result_files/{jobstorage_object_name}")
                arrow_table = self.internal_dict['jobstorage_dict_entry']['jobstorage_object_name']
                logging.info(f"Arrow Table Structure {arrow_table}")
                logging.info(f"Pandas Table {arrow_table.to_pandas().head(10)}")
                con = duckdb.connect()
                #logging.info(response.text)
                # query the Apache Arrow Table "my_arrow_table" and return as an Arrow Table
                # results = con.execute("SELECT * FROM arrow_table").df()
                # jsonstr = response.text
                # logging.info(f"DuckDb Table {results.head(1)}")
                # logging.info(con.execute("CREATE TABLE example (j JSON);"))
                # logging.info(con.execute(f"""INSERT INTO example VALUES
                # ('{jsonstr}');"""))
                # logging.info(con.execute("SELECT json(j) FROM example;").fetchdf())
                # logging.info(con.execute("SELECT json_valid(j) FROM example;").fetchdf())
                # logging.info(con.execute("SELECT json_structure(j) FROM example;").fetchdf())
               # multi_serializer = MultiSerializer()
               # df = multi_serializer.json_to_dataframe(json.loads(response.text))
               # logging.info(df.head(10))
               
