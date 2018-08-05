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
import threading
from flasgger import Swagger, swag_from
import DockerHelper as dHelper
import ZMQHelper as zmq
import MongoDBHelper as mg

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
  "host": "129.114.108.18:5000",
  "basePath": "",  # base bash for blueprint registration
  "schemes": [
    "http",
  ]
}

swagger = Swagger(app, template=template)

host_addr = None
dockerClient = None
pubSocket = None

m_addr = None
m_port = None
mongo_client = None
db_name = 'RESTfulSwarmDB'
workers_collection_name = 'WorkersInfo'
workers_resources = 'WorkersResourceInfo'
db = None
worker_col = None
worker_resource_col = None
inited_master = False


@app.route('/RESTfulSwarm/GM/init', methods=['GET'])
@swag_from('./Flasgger/init.yml')
def init():
    global pubSocket
    global inited_master
    try:
        pubSocket = zmq.bind('3100')
        initSwarmEnv()
        response = 'OK: Initialize Swarm environment succeed.'
        time.sleep(1)
        inited_master = True
        return response, 200
    except Exception as ex:
        response = 'Error: %s' % ex
        return response, 500


def initSwarmEnv():
    dHelper.initSwarm(dockerClient, advertise_addr=host_addr)
    app.logger.info('Init Swarm environment.')


def createOverlayNetwork(network, driver, subnet):
    dHelper.createNetwork(dockerClient, name=network, driver=driver, subnet=subnet)
    app.logger.info('Build overlay network: %s.' % network)


@app.route('/RESTfulSwarm/GM/requestJoin', methods=['POST'])
@swag_from('./Flasgger/requestJoin.yml', validation=True)
def requestJoin():
    data = request.get_json()
    hostname = data['hostname']
    worker_addr = data['address']
    if hostname is not None:
        if dHelper.checkNodeHostName(dockerClient, hostname):
            # configure nfs setting
            def configure_nfs():
                with open('/etc/exports', 'a') as f:
                    new_worker = '/var/nfs/RESTfulSwarm     %s(rw,sync,no_subtree_check)\n' % worker_addr
                    f.write(new_worker)
                # restart nfs
                os.system('sudo systemctl restart nfs-kernel-server')

            configure_nfs()
            init_worker_info(hostname, data['CPUs'], data['MemFree'])
            remote_addr = host_addr + ':2377'
            join_token = dHelper.getJoinToken()
            response = '%s join %s %s' % (hostname, remote_addr, join_token)
            pubSocket.send_string(response)
            app.logger.info('Send manager address and join token to worker node.')
            return response, 200
        else:
            response = 'Error: Node already in Swarm environment.'
            return response, 400
    else:
        response = 'Error: Please enter the IP address of node.'
        return response, 406


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
    if mg.filter_col(worker_col, 'hostname', hostname) is None:
        mg.insert_doc(worker_col, worker_info)

    # Init WorkerResource Collection
    mg.update_workers_resource_col(worker_col, hostname, worker_resource_col)


def newContainer(data):
    node = data['node']
    if dHelper.checkNodeHostName(dockerClient, node) is False:
        pubContent = '%s new_container %s' % (node, json.dumps(data))
        pubSocket.send_string(pubContent)
        app.logger.info('Create a new container in node %s.' % node)
        response = 'OK'
    else:
        response = 'Error: The node %s you specified is unavailable.' % node
        raise Exception(response)
    return response


@app.route('/RESTfulSwarm/GM/requestNewContainer', methods=['POST'])
@swag_from('./Flasgger/requestNewContainer.yml', validation=True)
def requestNewContainer():
    try:
        data = request.get_json()
        newContainer(data)
        return 'OK', 200
    except Exception as ex:
        return ex, 400


@app.route('/RESTfulSwarm/GM/requestNewJob', methods=['POST'])
def requestNewJob():
    data = request.get_json()
    # create overlay network if not exists
    if dHelper.verifyNetwork(dockerClient, data['job_info']['network']['name']):
        createOverlayNetwork(network=data['job_info']['network']['name'],
                             driver=data['job_info']['network']['driver'],
                             subnet=data['job_info']['network']['subnet'])
    try:
        # make directory for nfs
        job_nfs_path = '/var/nfs/RESTfulSwarm/%s' % data['job_name']
        os.mkdir(path=job_nfs_path)

        for task in list(data['job_info']['tasks'].keys()):
            data['job_info']['tasks'][task].update({'network': data['job_info']['network']['name']})

        # deploy job
        for item in data['job_info']['tasks'].values():
            newContainer(item)

        # update job status
        mg.update_doc(db[data['job_name']], 'job_name', data['job_name'], 'status', 'Deployed')
        mg.update_doc(db[data['job_name']], 'job_name', data['job_name'], 'start_time', time.time())

        # print('------------------------------')
        # print(data['job_name'])
        # print('------------------------------')

        # update task status
        for task in data['job_info']['tasks'].keys():
            filter_key = 'job_info.tasks.%s.container_name' % task
            target_key = 'job_info.tasks.%s.status' % task
            mg.update_doc(db[data['job_name']], filter_key, task, target_key, 'Deployed')

        return 'OK', 200
    except Exception as ex:
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/checkpointCons', methods=['POST'])
@swag_from('./Flasgger/checkpointCons.yml', validation=True)
def checkpointCons():
    data = request.get_json()
    for item in data:
        if dHelper.checkNodeHostName(dockerClient, item['node']):
            return 'Node %s is unavailable.' % item['node'], 400
        pubContent = '%s checkpoints %s' % (item['node'], json.dumps(item['containers']))
        pubSocket.send_string(pubContent)
        app.logger.info('Checkpoint containers %s on node %s' % (json.dumps(item['containers']), item['node']))
    return 'OK', 200


def containerMigration(data):
    src = data['from']
    dst = data['to']
    container = data['container']
    info = data['info']
    checkSrc = dHelper.checkNodeIP(dockerClient, src)
    checkDst = dHelper.checkNodeIP(dockerClient, dst)
    if checkSrc is False:
        response = 'Error: %s is an invalid address.' % src
        raise Exception(response)
    elif checkDst is False:
        response = 'Error: %s is an invalid address.' % dst
        raise Exception(response)
    else:
        jsonForm = {'src': src, 'dst': dst, 'container': container, 'info': info}
        pubContent = '%s migrate %s' % (src, json.dumps(jsonForm))
        pubSocket.send_string(pubContent)
        response = 'OK'
        app.logger.info('Migrate container %s from %s to %s.' % (container, src, dst))
    return response, 200


@app.route('/RESTfulSwarm/GM/requestMigrate', methods=['POST'])
@swag_from('./Flasgger/requestMigrate.yml', validation=True)
def requestMigrate():
    try:
        data = request.get_json()
        return containerMigration(data)
    except Exception as ex:
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/requestGroupMigration', methods=['POST'])
@swag_from('./Flasgger/requestGroupMigration.yml', validation=True)
def requestGroupMigration():
    try:
        data = request.get_json()
        for item in data:
            containerMigration(item)
        return 'OK', 200
    except Exception as ex:
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/requestLeave', methods=['POST'])
@swag_from('./Flasgger/requestLeave.yml', validation=True)
def requestLeave():
    hostname = request.get_json()['hostname']
    checkNode = dHelper.checkNodeHostName(client=dockerClient, host=hostname)
    if checkNode is False:
        pubContent = '%s leave' % hostname
        pubSocket.send_string(pubContent)
        # force delete the node on Manager side
        dHelper.removeNode(hostname)
        app.logger.info('Node %s left Swarm environment.' % hostname)
        return 'OK', 200
    else:
        return 'Error: Host %s is not in Swarm environment.' % hostname, 400


@app.route('/RESTfulSwarm/GM/requestUpdateContainer', methods=['POST'])
@swag_from('./Flasgger/requestUpdateContainer.yml', validation=True)
def requestUpdateContainer():
    newInfo = request.get_json()
    node = newInfo['node']
    container = newInfo['container_name']
    if dHelper.checkNodeHostName(client=dockerClient, host=node) is False:
        newInfo = json.dumps(newInfo)
        pubSocket.send_string('%s update %s' % (node, newInfo))
        app.logger.info('%s updated container %s' % (node, container))
        return 'OK', 200
    else:
        return 'Error: requested node %s is not in Swarm environment.' % node, 400


@app.route('/RESTfulSwarm/GM/getWorkerList', methods=['GET'])
@swag_from('./Flasgger/getWorkerList.yml')
def getWorkerList():
    nodes = dHelper.getNodeList(dockerClient)
    response = []
    for node in nodes:
        response.append(node.attrs)
    return jsonify(response), 200


def describeNode(hostname):
    nodeinfo = dHelper.getNodeInfo(dockerClient, hostname)
    if nodeinfo is None:
        return 'The requested node is unavailable.', 400
    else:
        return jsonify(nodeinfo), 200


@app.route('/RESTfulSwarm/GM/<hostname>/describeWorker', methods=['GET'])
@swag_from('./Flasgger/describeWorker.yml')
def describeWorker(hostname):
    return describeNode(hostname)


@app.route('/RESTfulSwarm/GM/<hostname>/describeManager', methods=['GET'])
@swag_from('./Flasgger/describeManager.yml')
def describeManager(hostname):
    return describeNode(hostname)


def main():
    # periodically collect unused network
    def prune_nw():
        while True:
            if inited_master:
                os.system('docker network prune --force --filter until=5m')
            time.sleep(5)

    prune_nw_thr = threading.Thread(target=prune_nw, args=())
    prune_nw_thr.daemon = True

    os.chdir('/home/%s/RESTfulSwarmLM/GlobalManager' % utl.getUserName())

    global m_addr
    global m_port
    global mongo_client
    global db
    global worker_col
    global worker_resource_col
    global host_addr
    global dockerClient

    with open('GlobalManagerInit.json') as f:
        data = json.load(f)
    g_addr = data['global_manager_addr']
    gport = data['global_manager_port']

    host_addr = g_addr
    dockerClient = dHelper.setClient()

    # mongodb
    m_addr = data['mongo_addr']
    m_port = data['mongo_port']

    mongo_client = mg.get_client(address=m_addr, port=m_port)
    db = mg.get_db(mongo_client, db_name)
    worker_col = mg.get_col(db, workers_collection_name)
    worker_resource_col = mg.get_col(db, workers_resources)

    os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.getUserName())

    app.run(host=g_addr, port=gport, debug=False)


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-ga', '--gaddr', type=str, help='The IP address of your Global Manager node.')
    # parser.add_argument('-gp', '--gport', type=int, default=5000, help='The port number you want to run your manager node.')
    # parser.add_argument('-ma', '--maddr', type=str, help='MongoDB address.')
    # parser.add_argument('-mp', '--mport', type=int, default=27017, help='MongoDB port.')
    # args = parser.parse_args()
    # g_addr = args.gaddr
    # gport = args.gport
    #
    # host_addr = utl.getHostIP()
    # dockerClient = dHelper.setClient()
    #
    # # mongodb
    # m_addr = args.maddr
    # m_port = args.mport

    main()
