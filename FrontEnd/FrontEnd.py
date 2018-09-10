#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
import json
from flask import *
import time
from flasgger import Swagger, swag_from

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mongodb_api as mg
from Messenger import Messenger
import utl
import SystemConstants

app = Flask(__name__)

db_address = None
db = None
messenger = None


def main():
    os.chdir('/home/%s/RESTfulSwarm/FrontEnd' % utl.get_username())

    global db_address
    global messenger
    global db

    with open('../ActorsInfo.json') as f:
        data = json.load(f)

    with open('../DBInfo.json') as f:
        db_info = json.load(f)

    db_client = mg.get_client(usr=db_info['user'], pwd=db_info['pwd'], db_name=db_info['db_name'],
                              address=db_info['address'], port=SystemConstants.MONGODB_PORT)
    db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)

    jm_address = data['JM']['address']
    messenger = Messenger(messenger_type='C/S', address=jm_address, port=SystemConstants.JM_PORT)

    fe_address = utl.get_local_address()

    template = {
        "swagger": "2.0",
        "info": {
            "title": "RESTfulSwarm",
            "description": "An RESTful application for Docker Swarm.",
            "contact": {
                "responsibleDeveloper": "Zhuangwei Kang",
                "email": "zhuangwei.kang@vanderbilt.edu"
            },
            "version": "0.0.1"
        },
        "host": '%s:%s' % (fe_address, SystemConstants.FE_PORT),
        "basePath": "",
        "schemes": [
            "http",
        ]
    }

    swagger = Swagger(app, template=template)

    @app.route('/RESTfulSwarm/FE/request_new_job', methods=['POST'])
    @swag_from('./Flasgger/FrontEnd.yml', validation=True)
    def request_new_job():
        global messenger
        # Write job data into MongoDB
        data = request.get_json()
        data.update({'submit_time': time.time()})
        col_name = data['job_name']
        m_col = mg.get_col(db, col_name)
        mg.insert_doc(m_col, data)

        # Notify job manager
        messenger.send(prompt='newJob', content=col_name)
        return 'OK', 200

    @app.route('/RESTfulSwarm/FE/switch_scheduler/<new_scheduler>', methods=['GET'])
    @swag_from('./Flasgger/SwitchScheduler.yml')
    def switch_scheduler(new_scheduler):
        global messenger
        # Notify Job Manager to switch scheduler
        messenger.send(prompt='SwitchScheduler', content=new_scheduler)
        return 'OK', 200

    os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())

    app.run(host=fe_address, port=SystemConstants.FE_PORT, debug=False)


if __name__ == '__main__':
    main()