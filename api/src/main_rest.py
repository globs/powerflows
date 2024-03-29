from flask import send_file
from flask import Flask, render_template, request, url_for, flash, redirect, jsonify
from flask import request
from flask import send_from_directory
from flask_cors import CORS
import os
import common.utils
import io, json 
import base64
import common.settings
import logging
from common.orchestration.jobcontrol.config import app
#from common.orchestration.jobs.jobfactory_decorator import FlowJobFactoryDecorator
from common.connections.engines.internal.metadata_manager import MetadataManager
from common.orchestration.jobs.job_factory import FlowJobFactory
import sqlite3
from werkzeug.exceptions import abort

def get_db_connection():
    conn = sqlite3.connect(common.settings.SQLLITE_DB_FILE)
    conn.row_factory = sqlite3.Row
    return conn

def get_secret(secret_name):
    conn = get_db_connection()
    post = conn.execute('SELECT * FROM tsecrets WHERE name = ?',
                        (secret_name,)).fetchone()
    conn.close()
    if post is None:
        abort(404)
    return post


#Actual flask code
celery=app
app = Flask(__name__)
app.config['SECRET_KEY'] = 'your secret key'
md_manager = MetadataManager()

cors = CORS(app)
app.debug = True

@app.route('/api/v1/docs',methods = ['POST', 'GET'])
def get_docs():
    return 'docs to be done'

def dict_factory(cursor, row):
    d = {}
    for idx, col in enumerate(cursor.description):
        d[col[0]] = row[idx]
    return d

#routes for frontend
@app.route('/api/v1/getsecrets',methods = ['GET'])
def get_secrets():
    conn = get_db_connection()
    conn.row_factory = dict_factory
    secrets = conn.execute('SELECT * FROM tsecrets').fetchall()
    conn.close()
    return jsonify(secrets)



@app.route('/api/v1/gettraces',methods = ['GET'])
def get_traces():
    return jsonify(md_manager.getTraces())

# begin Web console
@app.route('/adminconsole')
def index():
    conn = get_db_connection()
    secrets = conn.execute('SELECT * FROM tsecrets').fetchall()
    conn.close()
    rows = md_manager.getTraces() 
    return render_template('index.html', secrets=secrets, traces=rows)

@app.route('/job_traces')
def job_traces():
    rows = md_manager.getTraces() 
    logging.info(f'job traces meta data {rows}')
    return render_template('jobtraces.html', traces=rows)



@app.route('/create_secret', methods=('GET', 'POST'))
def create_secret():
    if request.method == 'POST':
        title = request.form['name']
        content = request.form['json_secret']

        if not title:
            flash('Secret name is required!')
        else:
            conn = get_db_connection()
            conn.execute('INSERT INTO tsecrets (name, json_secret) VALUES (?, ?)',
                         (title, content))
            conn.commit()
            conn.close()
            return redirect(url_for('index'))
    return render_template('create_secret.html')

@app.route('/display_secret/<secret_name>')
def display_secret(secret_name):
    secret = get_secret(secret_name)
    return render_template('secret.html', secret=secret)


@app.route('/edit_secret/<secret_name>', methods=('GET', 'POST'))
def edit_secret(secret_name):
    secret = get_secret(secret_name)

    if request.method == 'POST':
        name = request.form['name']
        json_secret = request.form['json_secret']

        if not name:
            flash('Secret name is required!')
        else:
            conn = get_db_connection()
            conn.execute('UPDATE tsecrets SET name = ?, json_secret = ?'
                         ' WHERE name = ?',
                         (name, json_secret, secret_name))
            conn.commit()
            conn.close()
            return redirect(url_for('index'))

    return render_template('edit_secret.html', secret=secret)


@app.route('/delete_secret/<secret_name>', methods=('POST',))
def delete_secret(secret_name):
    secret = get_secret(secret_name)
    conn = get_db_connection()
    conn.execute('DELETE FROM tsecrets WHERE name = ?', (secret_name,))
    conn.commit()
    conn.close()
    flash('"{}" was successfully deleted!'.format(secret['name']))
    return redirect(url_for('index'))


## Assets metadata management
 
@app.route('/create_asset', methods=('GET', 'POST'))
def create_asset():
    if request.method == 'POST':
        config = { "asset_name" : request.form['asset_name'],
        "asset_type" : request.form['asset_type'],
        "asset_yaml" : request.form['asset_yaml']
        }
        md_manager.createAsset(config)
        flash(f"{request.form['asset_name']} was created successfully!")
        return render_template('create_asset.html') 
    return render_template('create_asset.html') 

@app.route('/submit_job', methods=('GET', 'POST'))
def submit_job():
    if request.method == 'POST':
        asset_name = request.form['job_name']
        asset_type = 1
        job_yaml = md_manager.getAssetConfig({"asset_name": asset_name, "asset_type":asset_type})
        logging.info(f'getting yaml job to be posted {asset_name}: {job_yaml}')
        flash(f'"{asset_name}" Job was submitted successfully!')
        jf = FlowJobFactory(job_yaml)
        jf.executeJob()
        flash(f'"{asset_name}" Job was executed successfully!')
        return render_template('submit_job.html', jobs=jf.job_connections_config)
        #return redirect(url_for('display_job'))
    return render_template('submit_job.html')


@app.route('/display_job', methods=('GET', 'POST'))
def display_job():
    return render_template('display_job.html')

@app.route('/api/v1/download/<string:filename>')
def download_image(filename):
    return send_from_directory(os.getcwd() + "/static/js", path=filename, as_attachment=True)


#end web console


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

