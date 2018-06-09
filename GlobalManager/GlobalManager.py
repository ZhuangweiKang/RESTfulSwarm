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

host_addr = utl.getHostIP()
network = None
subnet = None
dockerClient = dHelper.setClient()
pubSocket = None


@app.route('/SwarmLMGM/manager/init', methods=['POST'])
def init():
    global network
    global subnet
    global pubSocket
    network = request.args.get('network')
    subnet = request.args.get('subnet')
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
    hostname = request.args.get('hostname')
    if hostname is not None:
        if dHelper.checkNodeHostName(dockerClient, hostname):
            remote_addr = host_addr + ':2377'
            join_token = dHelper.getJoinToken()
            response = '%s join %s %s' % (hostname, remote_addr, join_token)
            pubSocket.send(response)
            app.logger.info('Send manager address and join token to worker node.')
        else:
            response = 'Error: Node already in Swarm environment.'
    else:
        response = 'Error: Please enter the IP address of node.'
    return response


@app.route('/SwarmLMGM/worker/requestNewContainer', methods=['POST'])
def requestNewContainer():
    data = request.get_json()
    node = data['node']
    if dHelper.checkNodeHostName(dockerClient, node):
        pubContent = '%s new_container %s' % (node, json.dumps(data))
        pubSocket.send(pubContent)
        app.logger.info('Create a new container in node %s.' % node)
        return 'OK'
    else:
        return 'Error: The node you specified is unavailable.'


@app.route('/SwarmLMGM/worker/requestMigrate', methods=['POST'])
def requestMigrate():
    src = app.request.args.get('from')
    dst = app.request.args.get('to')
    container = app.request.args.get('container')
    checkSrc = dHelper.checkNodeIP(dockerClient, src)
    checkDst = dHelper.checkNodeIP(dockerClient, dst)
    if checkSrc is False:
        response = 'Error: %s is an invalid address.' % src
    elif checkDst is False:
        response = 'Error: %s is an invalid address.' % dst
    else:
        jsonForm = {'src': src, 'dst': dst, 'container': container}
        pubContent = '%s migrate %s' % (src, json.dumps(jsonForm))
        pubSocket.send(pubContent)
        response = 'OK'
        app.logger.info('Migrate container %s from %s to %s.' % (container, src, dst))
    return response


@app.route('/SwarmLMGM/worker/requestLeave', methods=['POST'])
def requestLeave():
    hostname = app.request.args.get('hostname')
    checkNode = dHelper.checkNodeHostName(client=dockerClient, host=hostname)
    if checkNode:
        pubContent = '%s leave' % hostname
        pubSocket.send(pubContent)
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
    'mem_limit': $mem_limit}
    '''
    if dHelper.checkNodeHostName(client=dockerClient, host=node):
        newInfo = json.dumps(newInfo)
        pubSocket.send('%s update %s' % (node, newInfo))
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
        return jsonify(nodeinfo)


@app.route('/SwarmLMGM/worker/<hostname>/describeWorker', methods=['GET'])
def describeWorker(hostname):
    return describeNode(hostname)


@app.route('/SwarmLMGM/manager/<hostname>/describeManager', methods=['GET'])
def describeManager(hostname):
    return describeNode(hostname)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', '--port', type=int, help='The port number you want to run your manager node.')
    args = parser.parse_args()
    port = args.port
    app.run(host=host_addr, port=port, debug=True)
