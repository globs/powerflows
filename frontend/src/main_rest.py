from flask import send_file
from flask import Flask, render_template, request, url_for, flash, redirect
from flask import request
from flask import send_from_directory
from flask_cors import CORS
import os
import io, json 
import base64
import logging
#from common.orchestration.jobs.jobfactory_decorator import FlowJobFactoryDecorator
from werkzeug.exceptions import abort
import requests



#Actual flask code
celery=app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your secret key'

cors = CORS(app)
app.debug = True



# begin Web console
@app.route('/adminconsole')
def index():
    secrets = requests.get("http://hulk1.vps.webdock.cloud:3333/api/v1/getsecrets").json()
    rows = requests.get("http://hulk1.vps.webdock.cloud:3333/api/v1/gettraces").json()
    return render_template('index.html', secrets=secrets, traces=rows)

@app.route('/job_traces')
def job_traces():
    rows = requests.get("http://hulk1.vps.webdock.cloud:3333/api/v1/gettraces").json()
    logging.info(f'job traces meta data {rows}')
    return render_template('jobtraces.html', traces=rows)



@app.route('/create_secret', methods=('GET', 'POST'))
def create_secret():
    return render_template('create_secret.html')

@app.route('/display_secret/<secret_name>')
def display_secret(secret_name):
    secret =
    return render_template('secret.html', secret=secret)


@app.route('/edit_secret/<secret_name>', methods=('GET', 'POST'))
def edit_secret(secret_name):
    secret = 
    return render_template('edit_secret.html', secret=secret)


@app.route('/delete_secret/<secret_name>', methods=('POST',))
def delete_secret(secret_name):
    flash(f'{secret['name']} was successfully deleted!')
    return redirect(url_for('index'))


## Assets metadata management
@app.route('/create_asset', methods=('GET', 'POST'))
def create_asset():
    return render_template('create_asset.html') 

@app.route('/submit_job', methods=('GET', 'POST'))
def submit_job():
    return render_template('submit_job.html')


@app.route('/display_job', methods=('GET', 'POST'))
def display_job():
    return render_template('display_job.html')

@app.route('/api/v1/download/<string:filename>')
def download_image(filename):
    return send_from_directory(os.getcwd() + "/static/js", path=filename, as_attachment=True)


#end web console




if __name__=="__main__":
    common.settings.init_logging()
    app.run(debug=True, host='0.0.0.0',port='3334')
#else:
#    gunicorn_logger = logging.getLogger('gunicorn.error')
#    app.logger.handlers = gunicorn_logger.handlers
#    app.logger.setLevel(gunicorn_logger.level)

