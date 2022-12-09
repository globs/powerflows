from flask import send_file
from flask import Flask
from flask import request
from flask_cors import CORS
import common.utils
import io, json 
import base64
import common.settings
import logging
from manager.load_config_manager import LoaderConfigManager
from common.orchestration.jobcontrol.config import app
from common.orchestration.jobs.jobfactory_decorator import FlowJobFactoryDecorator

#Actual flask code
celery=app
app = Flask(__name__)
cors = CORS(app)
app.debug = True
#loaderManager = LoaderConfigManager()

@app.route('/api/v1/docs',methods = ['POST', 'GET'])
def get_docs():
    return 'docs to be done'


@app.route('/api/v1/pingcelery',methods = ['POST', 'GET'])
def ping_celery():
    content_type = request.headers.get('Content-Type')
    json_dict = None
    if (content_type == 'application/json'):
        json_dict = request.json
        logging.info(f"Json dict: {json_dict}")
    uuid = common.utils.gen_uuid()
    celery.send_task(name='ping_async' ,args=(uuid,json_dict,), queue='qa', routing_key='qa.test')
    return 'dummy task submitted'

@app.route('/api/v1/jobdecorator',methods = ['POST', 'GET'])
def call_jobfactory_decorator():
    content_type = request.headers.get('Content-Type')
    config_rcv_type = None 
    if content_type == 'text/plain':
        config_rcv_type = common.settings.JOB_CONFIG_FORMAT_YAML
    elif content_type == 'application/json':
        config_rcv_type = common.settings.JOB_CONFIG_FORMAT_JSON
    raw_str = request.data  
    logging.info(raw_str)
    logging.info(f"raw text received: {raw_str}")
    uuid = common.utils.gen_uuid()
    job_creator = FlowJobFactoryDecorator(raw_str, config_rcv_type)
    return job_creator.config_json



@app.route('/api/v1/runjob_manual',methods = ['POST', 'GET'])
def run_job_manual():
    content_type = request.headers.get('Content-Type')
    if (content_type == 'application/json'):
        json_dict = request.json
        run_results = loaderManager.apply_json(json.dumps(json_dict))
        return run_results
    else:
        return f'Content-Type {content_type} not supported! application/json expected'


if __name__=="__main__":
    common.settings.init_logging()
    app.run(debug=True, host='0.0.0.0',port='3333')
#else:
#    gunicorn_logger = logging.getLogger('gunicorn.error')
#    app.logger.handlers = gunicorn_logger.handlers
#    app.logger.setLevel(gunicorn_logger.level)

