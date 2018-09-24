#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
import json
import time
import argparse
import traceback
import threading
from flask import *
from flasgger import Swagger, swag_from

import utl
import SystemConstants
import docker_api as docker
from Messenger import Messenger
import mongodb_api as mg

app = Flask(__name__)

gm_address = None
dockerClient = None
messenger = None

db_address = None
db_client = None
db = None
worker_col = None
worker_resource_col = None

job_buffer = []


@app.route('/RESTfulSwarm/GM/init', methods=['GET'])
@swag_from('./Flasgger/init.yml')
def init():
    global messenger
    try:
        messenger = Messenger(messenger_type='Pub/Sub', port=SystemConstants.GM_PUB_PORT)
        init_swarm_env()
        response = 'OK: Initialize Swarm environment succeed.'
        return response, 200
    except Exception as ex:
        response = 'Error: %s' % ex
        return response, 500


def init_swarm_env():
    docker.init_swarm(dockerClient, advertise_addr=gm_address)
    app.logger.info('Init Swarm environment.')


def create_overlay_network(network, driver, subnet):
    docker.create_network(dockerClient, name=network, driver=driver, subnet=subnet)
    app.logger.info('Build overlay network: %s.' % network)


@app.route('/RESTfulSwarm/GM/request_join', methods=['POST'])
@swag_from('./Flasgger/requestJoin.yml', validation=True)
def request_join():
    global messenger
    data = request.get_json()
    hostname = data['hostname']
    worker_address = data['address']
    if hostname is not None:
        if docker.check_node_ip(client=dockerClient, node_ip=worker_address) is False:

            # configure nfs setting
            def configure_nfs():
                with open('/etc/exports', 'a') as f:
                    new_worker = '/var/nfs/RESTfulSwarm/     %s(rw,sync,no_root_squash,no_subtree_check)\n' \
                                 % worker_address
                    f.write(new_worker)
                # restart nfs
                os.system('sudo systemctl restart nfs-kernel-server')
            configure_nfs()

            remote_address = gm_address + ':2377'
            join_token = docker.get_join_token()
            response = '%s join %s %s' % (worker_address, remote_address, join_token)
            messenger.publish(response)
            app.logger.info('Send manager address and join token to worker node.')

            # notify the worker node update "hostname" to worker's ID in swarm mode
            while True:
                try:
                    worker_id = docker.get_node_id(dockerClient, worker_address)
                    if worker_id is not None:
                        break
                except Exception:
                    pass
            print('Worker %s id is %s' % (worker_address, worker_id))
            worker_id_msg = '%s ID %s' % (worker_address, worker_id)
            messenger.publish(worker_id_msg)
            app.logger.info('Send worker\'s ID to worker node.')

            init_worker_info(worker_id, data['CPUs'], data['MemFree'])
            return response, 200
        else:
            response = 'Error: Node already in Swarm environment.'
            app.logger.error(response)
            return response, 400
    else:
        response = 'Error: Please enter the IP address of node.'
        app.logger.error(response)
        return response, 406


def init_worker_info(hostname, cores_num, mem_free):
    cores = {}
    for i in range(cores_num):
        cores.update({str(i): False})

    worker_info = {
        'hostname':  hostname,
        'CPUs': cores,
        'MemFree': mem_free
    }
    # Write initial worker information into database
    if mg.filter_col(worker_col, 'hostname', hostname) is None:
        mg.insert_doc(worker_col, worker_info)

    # Init WorkerResource Collection
    mg.update_workers_resource_col(worker_col, hostname, worker_resource_col)


def new_container(data):
    global messenger
    node = data['node']
    if docker.check_node_hostname(dockerClient, node) is False:
        msg = '%s new_container %s' % (node, json.dumps(data))
        messenger.publish(msg)
        app.logger.info('Create a new container in node %s.' % node)
        response = 'OK'
    else:
        response = 'Error: The node %s you specified is unavailable.' % node
        raise Exception(response)
    return response


@app.route('/RESTfulSwarm/GM/request_new_task', methods=['POST'])
@swag_from('./Flasgger/requestNewContainer.yml', validation=True)
def request_new_task():
    try:
        data = request.get_json()
        new_container(data)
        return 'OK', 200
    except Exception as ex:
        return ex, 400


@app.route('/RESTfulSwarm/GM/request_new_job', methods=['POST'])
def request_new_job():
    data = request.get_json()
    # create overlay network if not exists
    if docker.verify_network(dockerClient, data['job_info']['network']['name']):
        create_overlay_network(network=data['job_info']['network']['name'],
                               driver=data['job_info']['network']['driver'],
                               subnet=data['job_info']['network']['subnet'])

    try:
        # make directory for nfs
        nfs_master_path = '/var/nfs/RESTfulSwarm/%s' % data['job_name']
        os.mkdir(path=nfs_master_path)

        for _task in data['job_info']['tasks']:
            data['job_info']['tasks'][_task].update({'network': data['job_info']['network']['name']})

        # deploy job
        for _task in list(data['job_info']['tasks'].values()):
            new_container(_task)

        # update job status
        mg.update_doc(db[data['job_name']], 'job_name', data['job_name'], 'status', 'Deployed')
        mg.update_doc(db[data['job_name']], 'job_name', data['job_name'], 'start_time', time.time())

        # update task status
        for task in data['job_info']['tasks'].keys():
            filter_key = 'job_info.tasks.%s.container_name' % task
            target_key = 'job_info.tasks.%s.status' % task
            mg.update_doc(db[data['job_name']], filter_key, task, target_key, 'Deployed')

        job_buffer.append(data['job_name'])
        return 'OK', 200
    except Exception as ex:
        traceback.print_exc(file=sys.stderr)
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/checkpoint_cons', methods=['POST'])
@swag_from('./Flasgger/checkpointCons.yml', validation=True)
def checkpoint_cons():
    global messenger
    data = request.get_json()
    for item in data:
        if docker.check_node_hostname(dockerClient, item['node']):
            return 'Node %s is unavailable.' % item['node'], 400
        msg = '%s checkpoints %s' % (item['node'], json.dumps(item['containers']))
        messenger.publish(msg)
        app.logger.info('Checkpoint containers %s on node %s' % (json.dumps(item['containers']), item['node']))
    return 'OK', 200


def container_migration(data):
    global messenger
    src = data['from']
    dst = data['to']
    container = data['container']
    info = data['info']
    check_src = docker.check_node_ip(dockerClient, src)
    check_dst = docker.check_node_ip(dockerClient, dst)
    if check_src is False:
        response = 'Error: %s is an invalid address.' % src
        raise Exception(response)
    elif check_dst is False:
        response = 'Error: %s is an invalid address.' % dst
        raise Exception(response)
    else:
        json_obj = {'src': src, 'dst': dst, 'container': container, 'info': info}
        msg = '%s migrate %s' % (src, json.dumps(json_obj))
        messenger.publish(msg)
        app.logger.info('Migrate container %s from %s to %s.' % (container, src, dst))
    return 'OK', 200


@app.route('/RESTfulSwarm/GM/request_migrate', methods=['POST'])
@swag_from('./Flasgger/requestMigrate.yml', validation=True)
def request_migrate():
    try:
        data = request.get_json()
        return container_migration(data)
    except Exception as ex:
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/request_group_migration', methods=['POST'])
@swag_from('./Flasgger/requestGroupMigration.yml', validation=True)
def request_group_migration():
    try:
        data = request.get_json()
        for item in data:
            container_migration(item)
        return 'OK', 200
    except Exception as ex:
        app.logger.error(ex)
        return str(ex), 400


@app.route('/RESTfulSwarm/GM/request_leave', methods=['POST'])
@swag_from('./Flasgger/requestLeave.yml', validation=True)
def request_leave():
    hostname = request.get_json()['hostname']
    check_node = docker.check_node_hostname(client=dockerClient, host=hostname)
    if check_node is False:
        msg = '%s leave' % hostname
        messenger.publish(msg)
        # force delete the node on Manager side
        docker.remove_node(hostname)
        app.logger.info('Node %s left Swarm environment.' % hostname)
        return 'OK', 200
    else:
        response = 'Error: Host %s is not in Swarm environment.' % hostname
        app.logger.error(response)
        return response, 400


@app.route('/RESTfulSwarm/GM/request_update_container', methods=['POST'])
@swag_from('./Flasgger/requestUpdateContainer.yml', validation=True)
def request_update_container():
    global messenger
    update_info = request.get_json()
    node = update_info['node']
    container = update_info['container_name']
    if docker.check_node_hostname(client=dockerClient, host=node) is False:
        update_info = json.dumps(update_info)
        messenger.publish('%s update %s' % (node, update_info))
        app.logger.info('%s updated container %s' % (node, container))
        return 'OK', 200
    else:
        response = 'Error: requested node %s is not in Swarm environment.' % node
        return response, 400


@app.route('/RESTfulSwarm/GM/get_worker_list', methods=['GET'])
@swag_from('./Flasgger/getWorkerList.yml')
def get_worker_list():
    nodes = docker.get_node_list(dockerClient)
    response = list(map(lambda node: node.attrs, nodes))
    return jsonify(response), 200


def describe_node(hostname):
    node_info = docker.get_node_info(dockerClient, hostname)
    return 'The requested node is unavailable.', 400 if node_info is None else jsonify(node_info), 200


@app.route('/RESTfulSwarm/GM/<hostname>/describe_worker', methods=['GET'])
@swag_from('./Flasgger/describeWorker.yml')
def describe_worker(hostname):
    return describe_node(hostname)


@app.route('/RESTfulSwarm/GM/<hostname>/describe_manager', methods=['GET'])
@swag_from('./Flasgger/describeManager.yml')
def describe_manager(hostname):
    return describe_node(hostname)


def main():
    # clear /etc/exports to avoid duplicated nfs client
    with open('/etc/exports', 'w') as f:
        f.write('')

    os.chdir('/home/%s/RESTfulSwarm/GlobalManager' % utl.get_username())

    global db_address
    global db_client
    global db
    global worker_col
    global worker_resource_col
    global gm_address
    global dockerClient

    gm_address = utl.get_local_address()

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
        "host": '%s:%s' % (gm_address, SystemConstants.GM_PORT),
        "basePath": "",
        "schemes": [
            "http",
        ]
    }

    swagger = Swagger(app, template=template)

    dockerClient = docker.set_client()

    # mongodb
    with open('../DBInfo.json') as f:
        db_info = json.load(f)

    db_client = mg.get_client(usr=db_info['user'], pwd=db_info['pwd'], db_name=db_info['db_name'],
                              address=db_info['address'], port=SystemConstants.MONGODB_PORT)
    db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)
    worker_col = mg.get_col(db, SystemConstants.WorkersInfo)
    worker_resource_col = mg.get_col(db, SystemConstants.WorkersResourceInfo)

    # periodically prune unused network
    def prune_nw():
        while True:
            networks = []
            for job in job_buffer[:]:
                job_info = mg.filter_col(mg.get_col(db, job), 'job_name', job)
                if job_info is not None and job_info['status'] == 'Down':
                    networks.append(job_info['job_info']['network']['name'])
                    job_buffer.remove(job)
            docker.rm_networks(dockerClient, networks)
            print('Remove networks:', networks)
            time.sleep(60)

    prune_nw_thr = threading.Thread(target=prune_nw, args=())
    prune_nw_thr.daemon = True
    prune_nw_thr.start()

    os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())

    app.run(host=gm_address, port=SystemConstants.GM_PORT, debug=False)


if __name__ == '__main__':
    main()
