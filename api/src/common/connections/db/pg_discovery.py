

import common.settings
import deepcopy
from common.connections.engines.internal.metadata_manager import MetadataManager
import yaml

class PGDiscovery(object):
    def __init__(self, config):
        self.connexion = config['connection_pool']
        self.secret_name = config['secret_name']
        self.threadpool = None
        self.metadata_manager = MetadataManager()


    def __del__(self):
        pass

    def discovery_schema(self, config_map):
        db_name = config_map['database_name']
        schema_names = config_map['schema_names'].split(common.utils.csv_separator)
        for schema_name in schema_names:
            sql = f"SELECT table_name FROM information_schema.tables WHERE table_schema = '{schema_name}';"
            cursor = cnn.cursor()    
            cursor.execute(sql)
            rows = cursor.fetchall()
            for table_name in rows:
                config_table_scan = {}
                config_table_scan['db_name'] = db_name
                config_table_scan['schema_name'] = schema_name
                config_table_scan['table_name'] = table_name[0]
                self.discovery_table(config_table_scan)


    def discovery_table(self, config_map):
        table_name = config_map['table_name']
        sql = f"""
            SELECT column_name, ordinal_position, data_type
            FROM information_schema.columns
            WHERE table_schema = '{config_map['schema_name']}'
            AND table_name   = '{table_name}'
        """
        cursor = cnn.cursor()    
        cursor.execute(sql)
        rows = cursor.fetchall()
        for column in rows:
            column_asset = {}
            column_asset['asset_type'] = 'db_column'
            column_asset['secret_name'] = self.secret_name
            column_asset['db_name'] =  column_asset['db_name']
            column_asset['schema_name'] = config_map['schema_name']
            column_asset['table_name'] = table_name
            column_asset['column_name'] = column[0]
            column_asset['column_type'] = column_name[2]
            column_asset['column_position'] =  column_name[1]  
            asset_config = {"asset_name" : request.form['asset_name'],
            "asset_type" : column_asset['asset_type'],
            "asset_yaml" : yaml.dumps(column_asset)
            }
            self.metadata_manager                              
