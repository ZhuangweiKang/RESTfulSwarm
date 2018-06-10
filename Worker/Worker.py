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
        zmqHelper.addTopic(self.swarmSocket, self.host_address)

        # local storage
        # format: {$container : $containerInfo}
        self.storage = {}

    def listenManagerMsg(self):
        while True:
            msg = self.swarmSocket.recv()
            msg = msg.split()[1:]
            msg_type = msg[0]
            if msg_type == 'join':
                remote_addr = msg[1]
                join_token = msg[2]
                self.joinSwarm(remote_addr, join_token)
            elif msg_type == 'migrate':
                info = json.loads(msg[1])
                dst = info['dst']
                container = info['container']
                try:
                    lmController = LiveMigration(self.storage[container]['image'], container, self.storage[container]['network'], self.logger, self.dockerClient)
                    lmController.migrate(dst, self.storage[container]['command'])
                    del self.storage[container]
                except Exception as ex:
                    print(ex)
            elif msg_type == 'new_container':
                info = json.loads(msg[1])
                container_name = info['container_name']
                del info['node']
                self.storage.update({container_name: info})
                self.deleteOldContainer(container_name)
                self.pullImage(self.storage[container_name]['image'])
                self.runContainer(self.storage[container_name])
            elif msg_type == 'update':
                newInfo = json.loads(msg[1])
                container_name = newInfo['container_name']
                cpuset_cpus = newInfo['cpuset_cpus']
                mem_limits = newInfo['mem_limits']
                dHelper.updateContainer(self.dockerClient, container_name=container_name, cpuset_cpus=cpuset_cpus, mem_limit=mem_limits)
            elif msg_type == 'leave':
                dHelper.leaveSwarm(self.dockerClient)

    def listenWorkerMessage(self, port='3200'):
        lmController = LiveMigration()
        lmController.notMigrate(port)

    def joinSwarm(self, remote_addr, join_token):
        dHelper.joinSwarm(self.dockerClient, join_token, remote_addr)
        self.logger.info('Worker node join the Swarm environment.')

    def deleteOldContainer(self, name):
        if dHelper.checkContainer(self.dockerClient, name) is True:
            container = dHelper.getContainer(self.dockerClient, name)
            dHelper.deleteContainer(container)
            self.logger.info('Old container %s exists, deleting old container.' % name)

    def pullImage(self, image):
        if dHelper.checkImage(self.dockerClient, image) is False:
            dHelper.pullImage(self.dockerClient, image)
            self.logger.info('Image doesn\'t exist, building image.')

    def runContainer(self, containerInfo):
        container_name = containerInfo['name']
        image_name = containerInfo['image']
        network = containerInfo['network']
        command = containerInfo['command']
        cpuset_cpus = containerInfo['cpus']
        mem_limit = containerInfo['memory']
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
        print(requests.post(url=url, json={'hostname': self.hostname}))

    def requestLeaveSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/SwarmLMGM/worker/requestLeave'
        print(requests.post(url=url, json={'hostname': self.hostname}))


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Manager IP address.')
    args = parser.parse_args()
    manager_addr = args.address
    worker = Worker(manager_addr)
    worker.main()
    worker.requestJoinSwarm()