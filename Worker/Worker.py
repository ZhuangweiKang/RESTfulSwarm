#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utl
import threading
import requests
import json
from flask import *
import argparse
import random
import time
from LiveMigration import LiveMigration
import DockerHelper as dHelper
import ZMQHelper as zmqHelper


class Worker:
    def __init__(self, manager_addr, self_addr, discovery_addr, discovery_port, task_monitor_frequency):
        self.logger = utl.doLog('WorkerLogger', 'worker.log')
        self.swarmSocket = zmqHelper.connect(manager_addr, '3100')
        self.dockerClient = dHelper.setClient()
        self.manager_addr = manager_addr
        self.host_address = self_addr
        self.hostname = utl.getHostName()
        zmqHelper.subscribeTopic(self.swarmSocket, self.hostname)
        zmqHelper.subscribeTopic(self.swarmSocket, self.host_address)

        # local storage
        # format: {$container : $containerInfo}
        self.storage = {}

        self.discovery_addr = discovery_addr
        self.discovery_port = discovery_port
        self.task_monitor_frequency = task_monitor_frequency

    def monitor(self, discovery_addr, discovery_port='4000', frequency=20):
        time_flag = time.time()
        client = dHelper.setClient()
        time.sleep(frequency)
        hostname = utl.getHostName()
        socket = zmqHelper.csConnect(discovery_addr, discovery_port)
        while True:
            events = client.events(since=time_flag, until=time.time(), decode=True)
            msgs = []
            for event in events:
                if event['Type'] == 'container' and event['status'] == 'stop':
                    msg = hostname + ' ' + event['Actor']['Attributes']['name']
                    msgs.append(msg)

            # Notify discovery block to update MongoDB
            for msg in msgs:
                self.logger.info(msg)
                socket.send_string(msg)
                socket.recv_string()

            time_flag = time.time()
            time.sleep(frequency)

    def listenManagerMsg(self):
        while True:
            msg = self.swarmSocket.recv_string()
            msg = msg.split()[1:]
            msg_type = msg[0]
            if msg_type == 'join':
                remote_addr = msg[1]
                join_token = msg[2]
                self.joinSwarm(remote_addr, join_token)
            elif msg_type == 'checkpoints':
                data = json.loads(' '.join(msg[1:]))
                threads = []
                for i in range(0, len(data)):
                    checkpoint_name = data[i] + '_' + str(random.randint(1, 1000))
                    container_id = dHelper.getContainerID(self.dockerClient, data[i])
                    thr = threading.Thread(target=dHelper.checkpoint, args=(checkpoint_name, container_id, True, ))
                    thr.setDaemon(True)
                    threads.append(thr)

                for thr in threads:
                    thr.start()
            elif msg_type == 'migrate':
                info = json.loads(' '.join(msg[1:]))
                print(info)
                dst = info['dst']
                container = info['container']
                container_info = info['info']
                container_info['node'] = self.hostname
                try:
                    lmController = LiveMigration(image=self.storage[container]['image'], name=container,
                                                 network=self.storage[container]['network'], logger=self.logger,
                                                 dockerClient=self.dockerClient)
                    lmController.migrate(dst_addr=dst, port='3200', cmd=self.storage[container]['command'],
                                         container_detail=container_info)
                    del self.storage[container]
                except Exception as ex:
                    print(ex)
            elif msg_type == 'new_container':
                info = json.loads(' '.join(msg[1:]))
                container_name = info['container_name']
                del info['node']
                self.storage.update({container_name: info})
                self.deleteOldContainer(container_name)
                self.pullImage(self.storage[container_name]['image'])
                self.runContainer(self.storage[container_name])
            elif msg_type == 'update':
                newInfo = json.loads(' '.join(msg[1:]))
                container_name = newInfo['container_name']
                cpuset_cpus = newInfo['cpuset_cpus']
                mem_limit = newInfo['mem_limit']
                dHelper.updateContainer(self.dockerClient, container_name=container_name, cpuset_cpus=cpuset_cpus, mem_limit=mem_limit)
                self.logger.info('Updated cpuset_cpus to %s, mem_limits to %s' % (cpuset_cpus, mem_limit))
            elif msg_type == 'leave':
                dHelper.leaveSwarm(self.dockerClient)
                self.logger.info('Leave Swarm environment.')

    def listenWorkerMessage(self, port='3200'):
        lmController = LiveMigration(logger=self.logger, dockerClient=self.dockerClient, storage=self.storage)
        lmController.notMigrate(port)

    def joinSwarm(self, remote_addr, join_token):
        dHelper.joinSwarm(self.dockerClient, join_token, remote_addr)
        self.logger.info('Worker node join the Swarm environment.')

    def deleteOldContainer(self, name):
        if dHelper.checkContainer(self.dockerClient, name):
            self.logger.info('Old container %s exists, deleting old container.' % name)
            container = dHelper.getContainer(self.dockerClient, name)
            dHelper.deleteContainer(container)

    def pullImage(self, image):
        if dHelper.checkImage(self.dockerClient, image) is False:
            self.logger.info('Image doesn\'t exist, pulling image.')
            dHelper.pullImage(self.dockerClient, image)
        else:
            self.logger.info('Image already exists.')

    def runContainer(self, containerInfo):
        container_name = containerInfo['container_name']
        image_name = containerInfo['image']
        network = containerInfo['network']
        command = containerInfo['command']
        cpuset_cpus = containerInfo['cpuset_cpus']
        mem_limit = containerInfo['mem_limit']
        detach = containerInfo['detach']
        ports = containerInfo['ports']
        volumes = containerInfo['volumes']
        environment = containerInfo['environment']
        container = dHelper.runContainer(self.dockerClient,
                                         image=image_name,
                                         name=container_name,
                                         detach=detach,
                                         network=network,
                                         command=command,
                                         cpuset_cpus=cpuset_cpus,
                                         mem_limit=mem_limit,
                                         ports=ports,
                                         volumes=volumes,
                                         environment=environment)
        self.logger.info('Container %s is running.' % container_name)
        return container

    def main(self):
        migrateThr = threading.Thread(target=self.listenManagerMsg, args=())
        notMigrateThr = threading.Thread(target=self.listenWorkerMessage, args=())
        task_monitor_thr = threading.Thread(target=self.monitor,
                                            args=(self.discovery_addr, self.discovery_port, self.task_monitor_frequency,))
        task_monitor_thr.start()
        migrateThr.start()
        notMigrateThr.start()

    def requestJoinSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/RESTfulSwarm/GM/requestJoin'
        json_info = {
            'hostname': self.hostname,
            'CPUs': utl.get_total_cores(),
            'MemFree': utl.get_total_mem()
        }

        print(requests.post(url=url, json=json_info).content)

    def requestLeaveSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/RESTfulSwarm/GM/requestLeave'
        print(requests.post(url=url, json={'hostname': self.hostname}).content)


if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-ga', '--gaddr', type=str, help='Global Manager IP address.')
    parser.add_argument('-sa', '--self_addr', type=str, help='Self IP address')
    parser.add_argument('-da', '--discovery_addr', type=str, help='Discovery server address.')
    parser.add_argument('-dp', '--discovery_port', type=str, default='4000', help='Discovery server port number.')
    parser.add_argument('-f', '--frequency', type=int, default=20, help='Worker node task monitor frequency (s).')
    args = parser.parse_args()
    manager_addr = args.gaddr
    self_addr = args.self_addr
    discovery_addr = args.discovery_addr
    discovery_port = args.discovery_port
    frequency = args.frequency
    '''

    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', type=str, default='Worker1Init.json', help='Worker node init json file path.')
    args = parser.parse_args()
    worker_init_json = args.file
    with open(worker_init_json) as f:
        data = json.load(f)
    manager_addr = data['global_manager_addr']
    self_addr = data['worker_address']
    discovery_addr = data['discovery_addr']
    discovery_port = data['discovery_port']
    frequency = data['frequency']

    worker = Worker(manager_addr, self_addr, discovery_addr, discovery_port, frequency)
    worker.main()
    worker.requestJoinSwarm()