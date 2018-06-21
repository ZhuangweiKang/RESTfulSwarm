#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang
#
# FrontEnd is only responsible for storing job information into database and sending notification to JobManager

import os
import sys
from flask import *
import MongoDBHelper as mhelper
import time
import ZMQHelper as zmq
import argparse
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

app = Flask(__name__)

m_addr = None
m_port = None
m_db = None
socket = None


@app.route('/RESTfulSwarm/FE/requestNewJob', methods=['POST'])
def requestNewJob():
    global m_addr
    global m_port
    global m_db

    # Write job data into MongoDB
    data = request.get_json()
    data.update({'TimeStamp': time.time()})
    col_name = data['job_name']
    m_col = mhelper.get_col(m_db, col_name)
    mhelper.insert_doc(m_col, data)

    time.sleep(1)

    # Notify job manager
    msg = 'newJob %s' % col_name
    socket.send_string(msg)
    socket.recv_string()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ma', '--maddr', type=str, help='MongoDB node address.')
    parser.add_argument('-mp', '--mport', type=str, help='MongoDB node port number.')
    parser.add_argument('-db', '--database', type=str, help='MongoDB database name.')
    args = parser.parse_args()

    # db
    m_addr = args.maddr
    m_port = args.mport
    m_db = args.database
    m_client = mhelper.get_client(address=m_addr, port=m_port)
    m_db = mhelper.get_db(m_client, m_db)

    socket = zmq.csConnect(m_addr, m_port)