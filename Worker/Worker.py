#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utl
import threading
import requests
import json
import time
from flask import *
import argparse
from LiveMigration import LiveMigration
import DockerHelper as dHelper
import ZMQHelper as zmqHelper


class Worker:
    def __init__(self, manager_addr):
        self.logger = utl.doLog('WorkerLogger', 'worker.log')
        self.swarmSocket = zmqHelper.connect(manager_addr, '3100')
        self.dockerClient = dHelper.setClient()
        self.manager_addr = manager_addr
        self.host_address = utl.getHostIP()
        self.hostname = utl.getHostName()
        zmqHelper.subscribeTopic(self.swarmSocket, self.hostname)
        zmqHelper.subscribeTopic(self.swarmSocket, self.host_address)

        # local storage
        # format: {$container : $containerInfo}
        self.storage = {}

    def listenManagerMsg(self):
        while True:
            msg = self.swarmSocket.recv_string()
            msg = msg.split()[1:]
            msg_type = msg[0]
            if msg_type == 'join':
                remote_addr = msg[1]
                join_token = msg[2]
                self.joinSwarm(remote_addr, join_token)
            elif msg_type == 'migrate':
                info = json.loads(' '.join(msg[1:]))
                dst = info['dst']
                container = info['container']
                try:
                    lmController = LiveMigration(image=self.storage[container]['image'], name=container, network=self.storage[container]['network'], logger=self.logger, dockerClient=self.dockerClient)
                    lmController.migrate(dst_addr=dst, port='3200', cmd=self.storage[container]['command'])
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
                mem_limits = newInfo['mem_limits']
                dHelper.updateContainer(self.dockerClient, container_name=container_name, cpuset_cpus=cpuset_cpus, mem_limit=mem_limits)
                self.logger.info('Updated cpuset_cpus to %s, mem_limits to %s' % (cpuset_cpus, mem_limits))
            elif msg_type == 'leave':
                dHelper.leaveSwarm(self.dockerClient)
                self.logger.info('Leave Swarm environment.')

    def listenWorkerMessage(self, port='3200'):
        lmController = LiveMigration()
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
        container = dHelper.runContainer(self.dockerClient,
                                         image=image_name,
                                         name=container_name,
                                         detach=detach,
                                         network=network,
                                         command=command,
                                         cpuset_cpus=cpuset_cpus,
                                         mem_limit=mem_limit,
                                         ports=ports,
                                         volumes=volumes)
        self.logger.info('Container %s is running.' % container_name)
        return container

    def main(self):
        migrateThr = threading.Thread(target=self.listenManagerMsg, args=())
        notMigrateThr = threading.Thread(target=self.listenWorkerMessage, args=())
        migrateThr.start()
        notMigrateThr.start()

    def requestJoinSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/SwarmLMGM/worker/requestJoin'
        print(requests.post(url=url, json={'hostname': self.hostname}).content)

    def requestLeaveSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/SwarmLMGM/worker/requestLeave'
        print(requests.post(url=url, json={'hostname': self.hostname}).content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Manager IP address.')
    args = parser.parse_args()
    manager_addr = args.address
    worker = Worker(manager_addr)
    worker.main()
    worker.requestJoinSwarm()