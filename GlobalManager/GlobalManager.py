#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
from flask import *
import time
import utl
import argparse
import DockerHelper as dHelper
import ZMQHelper as zmq
import MongoDBHelper as mg

app = Flask(__name__)

host_addr = None
dockerClient = None
pubSocket = None

m_addr = None
m_port = None
mongo_client = None
db_name = 'RESTfulSwarmDB'
workers_collection_name = 'WorkersInfo'
db = None
worker_col = None


@app.route('/RESTfulSwarm/GM/init', methods=['GET'])
def init():
    global pubSocket
    try:
        pubSocket = zmq.bind('3100')
        initSwarmEnv()
        response = 'OK: Initialize Swarm environment and create network succeed.'
    except Exception as ex:
        response = 'Error: %s' % ex
    return response


def initSwarmEnv():
    dHelper.initSwarm(dockerClient, advertise_addr=host_addr)
    app.logger.info('Init Swarm environment.')


def createOverlayNetwork(network, subnet):
    dHelper.createNetwork(dockerClient, name=network, driver='overlay', subnet=subnet)
    app.logger.info('Build overlay network: %s.' % network)


@app.route('/RESTfulSwarm/GM/requestJoin', methods=['POST'])
def requestJoin():
    data = request.get_json()
    hostname = data['hostname']
    if hostname is not None:
        if dHelper.checkNodeHostName(dockerClient, hostname):
            init_worker_info(hostname, data['CPUs'], data['MemFree'])
            remote_addr = host_addr + ':2377'
            join_token = dHelper.getJoinToken()
            response = '%s join %s %s' % (hostname, remote_addr, join_token)
            pubSocket.send_string(response)
            app.logger.info('Send manager address and join token to worker node.')
        else:
            response = 'Error: Node already in Swarm environment.'
    else:
        response = 'Error: Please enter the IP address of node.'
    return response


def init_worker_info(hostname, cpus, memfree):
    cores = {}
    for i in range(cpus):
        cores.update({str(i): False})
    worker_info = {
        'hostname':  hostname,
        'CPUs': cores,
        'MemFree': memfree
    }
    # Write initial worker information into database
    mg.insert_doc(worker_col, worker_info)


def newContainer(data):
    node = data['node']
    if dHelper.checkNodeHostName(dockerClient, node) is False:
        pubContent = '%s new_container %s' % (node, json.dumps(data))
        pubSocket.send_string(pubContent)
        app.logger.info('Create a new container in node %s.' % node)
        response = 'OK'
    else:
        response = 'Error: The node you specified is unavailable.'
    return response


@app.route('/RESTfulSwarm/GM/requestNewContainer', methods=['POST'])
def requestNewContainer():
    data = request.get_json()
    newContainer(data)


@app.route('/RESTfulSwarm/GM/requestNewJob', methods=['POST'])
def requestNewJob():
    data = request.get_json()
    # create overlay network if not exists
    if dHelper.verifyNetwork(dockerClient, data['job_info']['network']):
        createOverlayNetwork(data['job_info']['network']['name'], data['job_info']['network']['subnet'])
    time.sleep(1)

    # deploy job
    for item in data['job_info']['tasks'].values():
        newContainer(item)

    return 'OK'


@app.route('/RESTfulSwarm/GM/checkpointCons', methods=['POST'])
def checkpointCons():
    data = request.get_json()
    for item in data:
        if dHelper.checkNodeHostName(dockerClient,  item['node']):
            return 'Node %s is unavailable.' % item['node']
        pubContent = '%s checkpoints %s' % (item['node'], json.dumps(item['containers']))
        pubSocket.send_string(pubContent)
        app.logger.info('Checkpoint containers %s on node %s' % (json.dumps(item['containers']), item['node']))
    return 'OK'


@app.route('/RESTfulSwarm/GM/requestMigrate', methods=['POST'])
def requestMigrate():
    data = request.get_json()
    src = data['from']
    dst = data['to']
    container = data['container']
    info = data['info']
    checkSrc = dHelper.checkNodeIP(dockerClient, src)
    checkDst = dHelper.checkNodeIP(dockerClient, dst)
    if checkSrc is False:
        response = 'Error: %s is an invalid address.' % src
    elif checkDst is False:
        response = 'Error: %s is an invalid address.' % dst
    else:
        jsonForm = {'src': src, 'dst': dst, 'container': container, 'info': info}
        pubContent = '%s migrate %s' % (src, json.dumps(jsonForm))
        pubSocket.send_string(pubContent)
        response = 'OK'
        app.logger.info('Migrate container %s from %s to %s.' % (container, src, dst))
    return response


@app.route('/RESTfulSwarm/GM/requestLeave', methods=['POST'])
def requestLeave():
    hostname = request.get_json()['hostname']
    checkNode = dHelper.checkNodeHostName(client=dockerClient, host=hostname)
    if checkNode is False:
        pubContent = '%s leave' % hostname
        pubSocket.send_string(pubContent)
        # force delete the node on Manager side
        dHelper.removeNode(hostname)
        app.logger.info('Node %s left Swarm environment.' % hostname)
        return 'OK'
    else:
        return 'Error: Host %s is not in Swarm environment.' % hostname


@app.route('/RESTfulSwarm/GM/requestUpdateContainer', methods=['POST'])
def requestUpdateContainer():
    newInfo = request.get_json()
    node = newInfo['node']
    container = newInfo['container_name']
    '''
    newInfo data format: 
    {'node': $node_name,
    'container_name': $container_name,
    'cpuset_cpus': $cpuset_cpus,
    'mem_limits': $mem_limits}
    '''
    if dHelper.checkNodeHostName(client=dockerClient, host=node) is False:
        newInfo = json.dumps(newInfo)
        pubSocket.send_string('%s update %s' % (node, newInfo))
        app.logger.info('%s updated container %s' % (node, container))
        return 'OK'
    else:
        return 'Error: requested node %s is not in Swarm environment.' % node


@app.route('/RESTfulSwarm/GM/getWorkerList', methods=['GET'])
def getWorkerList():
    nodes = dHelper.getNodeList(dockerClient)
    response = {}
    for node in nodes:
        response.update(json.dumps(node.attrs))
    return jsonify(response)


def describeNode(hostname):
    nodeinfo = dHelper.getNodeInfo(dockerClient, hostname)
    if nodeinfo is None:
        return 'The requested node is unavailable.'
    else:
        return nodeinfo


@app.route('/RESTfulSwarm/GM/<hostname>/describeWorker', methods=['GET'])
def describeWorker(hostname):
    return describeNode(hostname)


@app.route('/RESTfulSwarm/GM/<hostname>/describeManager', methods=['GET'])
def describeManager(hostname):
    return describeNode(hostname)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ga', '--gaddr', type=str, help='The IP address of your Global Manager node.')
    parser.add_argument('-gp', '--gport', type=int, default=5000, help='The port number you want to run your manager node.')
    parser.add_argument('-ma', '--maddr', type=str, help='MongoDB address.')
    parser.add_argument('-mp', '--mport', type=int, default=27017, help='MongoDB port.')
    args = parser.parse_args()
    g_addr = args.gaddr
    gport = args.gport

    host_addr = utl.getHostIP()
    dockerClient = dHelper.setClient()

    # mongodb
    m_addr = args.maddr
    m_port = args.mport
    mongo_client = mg.get_client(m_addr, m_port)
    db = mg.get_db(mongo_client, db_name)
    worker_col = mg.get_col(db, workers_collection_name)

    app.run(host=g_addr, port=gport, debug=True)
