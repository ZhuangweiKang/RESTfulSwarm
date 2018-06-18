#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from flask import *
import MongoDBHelper as mhelper
import time
import ZMQHelper as zmq
import argparse

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
    # Write data into MongoDB
    data = request.get_json()
    col_name = data['job_info']['job_name']
    m_client = mhelper.get_client(address=m_addr, port=m_port)
    m_db = mhelper.get_db(m_client, m_db)
    m_col = mhelper.get_col(m_db, col_name)
    mhelper.insert_doc(m_col, data)

    time.sleep(1)

    # TODO：Apply Schedule Algorithm here
    target_col = scheduel()

    # Notify job manager
    msg = '%s %s' % (m_db, target_col)
    socket.send_string(msg)
    socket.recv_string()


def scheduel():
    return None


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ma', '--maddr', type=str, help='MongoDB node address.')
    parser.add_argument('-mp', '--mport', type=str, help='MongoDB node port number.')
    parser.add_argument('-db', '--database', type=str, help='MongoDB database name.')
    args = parser.parse_args()
    m_addr = args.maddr
    m_port = args.mport
    m_db = args.database
    socket = zmq.csConnect(m_addr, m_port)