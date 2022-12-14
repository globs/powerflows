import uuid
import logging
import common.settings
import yaml
import json

def gen_uuid():
    return str(uuid.uuid4())[:32]

def clean_csv_value( value):
    res = str(value)
    if value is None:
        res = r'\N'
    else:
        res = res.replace("'", "''" ).replace('\n', '\\n')  
    return res

def list_depth(l):
    if isinstance(l, list):
        return 1 + max(list_depth(item) for item in l)
    else:
        return 0

def json_list_to_csv(json_list):
    res = ""
    for res_element in json_list:
        if  isinstance(res_element, list) and list_depth(res_element) == 1:
            res = res + common.settings.csv_separator.join(map(common.utils.clean_csv_value, res_element)) + "\n"
        else:
            for list_element in res_element:
                logging.debug(f"Json element type {type(list_element)}")
                if isinstance(list_element, list):
                    res = res + common.settings.csv_separator.join(map(common.utils.clean_csv_value, list_element)) + "\n"
                elif isinstance(list_element, dict):
                    res = res + json_to_csv(res_element)+ "\n"
                else:
                    logging.error(f"Error type of json object not recognized: {type(list_element)} data: {list_element}")
    return res

def json_to_csv(data):
        logging.debug(f"{data}")
        res_tuple = ()
        res_struct = ()
        res_csv = ""
        for key in data:
            logging.debug(type(data[key]))
            if isinstance(data[key], dict):
                logging.debug('dict field')
                for subkey in data[key]:
                    res_tuple = res_tuple + (str(data[key][subkey]),)    
            elif isinstance(data[key], list):
                if key == 'b' or key == 'a':
                    tuple_save = res_tuple
                    res_tuple = res_tuple + (key,) 
                    for val in data[key]:
                        res_tuple = res_tuple + (str(val[0]),) 
                        res_tuple = res_tuple + (str(val[1]),) 
                        res_csv = res_csv  + common.settings.csv_separator.join(map(common.utils.clean_csv_value, res_tuple))  + "\n"
                        res_tuple = tuple_save + (key,)
                    res_tuple = tuple_save
            else: 
                logging.debug('Simple field')
                res_tuple = res_tuple + (str(data[key]),) 
            #for when it is not a depth json
        if len(res_csv) == 0:   
            res_csv = common.settings.csv_separator.join(map(clean_csv_value, res_tuple))  + "\n"
        logging.debug(f"Parse result in csv data : {res_csv}")
        return res_csv

def stryaml_to_json(str_yaml):
    logging.info('converting yaml ')
    res =yaml.safe_load(str_yaml)
    return res

def strjson_to_json(str_json):
    res =json.dumps(str_json)
    return res

def yaml_to_json(yaml_file, outjsonfilepath):
    with open(yaml_file, 'r') as file:
        configuration = yaml.safe_load(file)
    with open(outjsonfilepath, 'w') as json_file:
        json.dump(configuration, json_file)
    output = json.dumps(json.load(open('config.json')), indent=2)
    return output

def json_to_yaml(json_file, outyamlfilepath):
    with open('config.json', 'r') as file:
        configuration = json.load(file)
    with open('config.yaml', 'w') as yaml_file:
        yaml.dump(configuration, yaml_file)
    with open('config.yaml', 'r') as yaml_file:
        return yaml_file.read()