from common.orchestration.jobcontrol.config import app
import logging, os, traceback
import common.settings

celery = app
#logging_conf_path = os.path.normpath(os.path.join(os.path.dirname(__file__), '../logging.conf'))
#logging.config.fileConfig(logging_conf_path)
#log = logging.getLogger('celery_tasks')

@app.task(name='ping_async')
def cointegration(uuid, json_dict):
    try: 
        logging.info(f'starting celery worker{uuid}, {json_dict}' )
    except Exception as e:
        logging.error(f'error in task wrapper {uuid}')
        logging.error(e)

