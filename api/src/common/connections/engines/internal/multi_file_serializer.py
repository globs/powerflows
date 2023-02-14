
import logging
import pyarrow
import duckdb
import common.utils
from copy import deepcopy
import pandas



managed_formats = ['csv', 'parquet', 'arrow', 'yaml', 'json', 'orc', 'pandas', 'polars']


#TODO create functions for files, string, objectstores
class MultiSerializer(object):
    def __init__(self):
        self.internal_storage = {}

    def getArrowIntermediaryData(self):
        pass

    def __del__(self):
        pass





    def cross_join(self, left, right):
        new_rows = [] if right else left
        for left_row in left:
            for right_row in right:
                temp_row = deepcopy(left_row)
                for key, value in right_row.items():
                    temp_row[key] = value
                new_rows.append(deepcopy(temp_row))
        return new_rows


    def flatten_list(self, data):
        for elem in data:
            if isinstance(elem, list):
                yield from self.flatten_list(elem)
            else:
                yield elem


    def json_to_dataframe(self, data_in):
        def flatten_json(data, prev_heading=''):
            if isinstance(data, dict):
                rows = [{}]
                for key, value in data.items():
                    rows = self.cross_join(rows, flatten_json(value, prev_heading + '.' + key))
            elif isinstance(data, list):
                rows = []
                for item in data:
                    [rows.append(elem) for elem in self.flatten_list(flatten_json(item, prev_heading))]
            else:
                rows = [{prev_heading[1:]: data}]
            return rows

        return pandas.DataFrame(flatten_json(data_in))

