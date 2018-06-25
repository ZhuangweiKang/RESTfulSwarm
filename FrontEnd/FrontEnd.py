#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang
#
# FrontEnd is only responsible for storing job information into database and sending notification to JobManager

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import *
import MongoDBHelper as mhelper
import time
import ZMQHelper as zmq
import argparse

app = Flask(__name__)

mongo_addr = None
mongo_port = None
mongo_db_name = 'RESTfulSwarmDB'
socket = None


@app.route('/RESTfulSwarm/FE/requestNewJob', methods=['POST'])
def requestNewJob():
    # Write job data into MongoDB
    data = request.get_json()
    data.update({'TimeStamp': time.time()})
    col_name = data['job_name']
    m_col = mhelper.get_col(mongo_db, col_name)
    mhelper.insert_doc(m_col, data)

    time.sleep(1)

    # Notify job manager
    msg = 'newJob %s' % col_name
    socket.send_string(msg)
    socket.recv_string()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='The FrontEnd node IP address.')
    parser.add_argument('-ma', '--mongo_addr', type=str, help='MongoDB node address.')
    parser.add_argument('-mp', '--mongo_port', type=int, default=27017, help='MongoDB node port number.')
    parser.add_argument('-ja', '--jm_addr', type=str, help='Job Manager address.')
    parser.add_argument('-jp', '--jm_port', type=str, default='2990', help='Job Manager port number that is used to receive job notification.')
    args = parser.parse_args()

    # db
    mongo_addr = args.mongo_addr
    mongo_port = args.mongo_port
    mongo_client = mhelper.get_client(address=mongo_addr, port=mongo_port)
    mongo_db = mhelper.get_db(mongo_client, mongo_db_name)

    # socket that is used to send job notification to job manager
    jm_addr = args.jm_addr
    jm_port = args.jm_port
    socket = zmq.csConnect(jm_addr, jm_port)

    fe_address = args.address

    app.run(host=fe_address, port=5000, debug=True)