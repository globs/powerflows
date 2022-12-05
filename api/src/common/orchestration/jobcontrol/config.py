from celery import Celery
from kombu import Queue
import os, logging


back_url='redis://redis:6379'
b_url='redis://redis:6379'
log = logging.getLogger('celery_config')
log.info(f'broker url {b_url}, backend_url {back_url}')
app = Celery('main_jobcontrol', backend=back_url, broker=b_url)

app.conf.task_queues = (
    Queue('qa', routing_key='qa.#'),
)
app.conf.task_default_exchange = 'tasks'
app.conf.task_default_exchange_type = 'topic'
app.conf.task_default_routing_key = 'qa.default'