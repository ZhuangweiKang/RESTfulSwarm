#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang
#
# FrontEnd is only responsible for storing job information into database and sending notification to JobManager

import os
import sys
import json
from flask import *
import time
import argparse
from flasgger import Swagger, swag_from

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import mongodb_api as mg
import zmq_api as zmq
import utl
import SystemConstants

app = Flask(__name__)

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
  "host": "129.114.108.18:5001",
  "basePath": "",  # base bash for blueprint registration
  "schemes": [
    "http",
  ]
}

swagger = Swagger(app, template=template)

__db_address__ = None
__db__ = None
__socket__ = None


@app.route('/RESTfulSwarm/FE/request_new_job', methods=['POST'])
@swag_from('FrontEnd.yml', validation=True)
def request_new_job():
    global __socket__
    # Write job data into MongoDB
    data = request.get_json()
    data.update({'submit_time': time.time()})
    col_name = data['job_name']
    m_col = mg.get_col(__db__, col_name)
    mg.insert_doc(m_col, data)

    # Notify job manager
    msg = 'newJob %s' % col_name
    __socket__.send_string(msg)
    __socket__.recv_string()
    return 'OK', 200


@app.route('/RESTfulSwarm/FE/switch_scheduler/<new_scheduler>', methods=['GET'])
@swag_from('SwitchScheduler.yml')
def switch_scheduler(new_scheduler):
    # Notify Job Manager to switch scheduler
    msg = 'SwitchScheduler %s' % new_scheduler
    __socket__.send_string(msg)
    __socket__.recv_string()
    return 'OK', 200


def main():
    os.chdir('/home/%s/RESTfulSwarmLM/FrontEnd' % utl.get_username())

    global __db_address__
    global __socket__
    global __db__

    with open('FrontEndInit.json') as f:
        data = json.load(f)

    __db_address__ = data['db_address']

    db_client = mg.get_client(address=__db_address__, port=SystemConstants.MONGODB_PORT)
    __db__ = mg.get_db(db_client, SystemConstants.MONGODB_NAME)

    jm_address = data['jm_address']
    jm_port = SystemConstants.JM_PORT
    __socket__ = zmq.cs_connect(jm_address, jm_port)

    fe_address = data['fe_address']

    os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.get_username())

    app.run(host=fe_address, port=SystemConstants.FE_PORT, debug=False)


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--FE', type=str, help='The FrontEnd node IP address.')
    # parser.add_argument('--db', type=str, help='MongoDB node address.')
    # parser.add_argument('--JM', type=str, help='Job Manager address.')
    # args = parser.parse_args()
    #
    # # db
    # db_address = args.db
    # db_client = mg.get_client(address=db_address, port=SystemConstants.MONGODB_PORT)
    # db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)
    #
    # # socket that is used to send job notification to job manager
    # jm_address = args.JM
    # socket = zmq.cs_connect(jm_address, SystemConstants.JM_PORT)
    # fe_address = args.FE
    #
    main()