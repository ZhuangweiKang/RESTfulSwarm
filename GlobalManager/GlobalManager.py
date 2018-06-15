#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utl
import json
from flask import *
import DockerHelper as dHelper
import argparse
import ZMQHelper as zmq


app = Flask(__name__)

host_addr = None
network = None
subnet = None
dockerClient = dHelper.setClient()
pubSocket = None


@app.route('/SwarmLMGM/manager/init', methods=['POST'])
def init():
    global network
    global subnet
    global pubSocket
    data = request.get_json()
    network = data['network']
    subnet = data['subnet']
    if network is None:
        response = 'Error: Please specify the network name.'
    elif subnet is None:
        response = 'Error: Please specify the subnet CIDR.'
    elif dHelper.verifyNetwork(dockerClient, network) is False:
        response = 'Error: Specified Network already exists in Swarm environment.'
    else:
        try:
            pubSocket = zmq.bind('3100')
            initSwarmEnv()
            createOverlayNetwork()
            response = 'OK: Initialize Swarm environment and create network succeed.'
        except Exception as ex:
            response = 'Error: %s' % ex
    return response


def initSwarmEnv():
    dHelper.initSwarm(dockerClient, advertise_addr=host_addr)
    app.logger.info('Init Swarm environment.')


def createOverlayNetwork():
    dHelper.createNetwork(dockerClient, name=network, driver='overlay', subnet=subnet)
    app.logger.info('Build overlay network: %s.' % network)


@app.route('/SwarmLMGM/worker/requestJoin', methods=['POST'])
def requestJoin():
    hostname = request.get_json()['hostname']
    if hostname is not None:
        if dHelper.checkNodeHostName(dockerClient, hostname):
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


@app.route('/SwarmLMGM/worker/requestNewContainer', methods=['POST'])
def requestNewContainer():
    data = request.get_json()
    newContainer(data)


@app.route('/SwarmLMGM/worker/requestNewJob', methods=['POST'])
def requestNewJob():
    data = request.get_json()
    for item in data['tasks']:
        newContainer(item)


@app.route('/SwarmLMGM/worker/checkpointCons', methods=['POST'])
def checkpointCons():
    data = request.get_json()
    for item in data:
        if dHelper.checkNodeHostName(dockerClient,  item['node']):
            return 'Node %s is unavailable.' % item['node']
        pubContent = '%s checkpoints %s' % (item['node'], json.dumps(item['containers']))
        pubSocket.send_string(pubContent)
        app.logger.info('Checkpoint containers %s on node %s' % (json.dumps(item['containers']), item['node']))
    return 'OK'


@app.route('/SwarmLMGM/worker/requestMigrate', methods=['POST'])
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


@app.route('/SwarmLMGM/worker/requestLeave', methods=['POST'])
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


@app.route('/SwarmLMGM/worker/requestUpdateContainer', methods=['POST'])
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


@app.route('/SwarmLMGM/manager/getWorkerList', methods=['GET'])
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


@app.route('/SwarmLMGM/worker/<hostname>/describeWorker', methods=['GET'])
def describeWorker(hostname):
    return describeNode(hostname)


@app.route('/SwarmLMGM/manager/<hostname>/describeManager', methods=['GET'])
def describeManager(hostname):
    return describeNode(hostname)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='The IP address of your Global Manager node.')
    parser.add_argument('-p', '--port', type=int, help='The port number you want to run your manager node.')
    args = parser.parse_args()
    host_addr = args.address
    port = args.port
    app.run(host=host_addr, port=port, debug=True)
