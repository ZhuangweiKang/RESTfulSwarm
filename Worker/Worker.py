#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utl
import threading
import requests
import json
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
                lmController = LiveMigration(self.storage[container]['image'], container, self.storage[container]['network'], self.logger, self.dockerClient)
                lmController.migrate(dst, self.storage[container]['command'])
            elif msg_type == 'new_container':
                info = msg[1]
                container_name = info['container_name']
                del info['node']
                del info['container_name']
                self.storage.update({container_name: info})
                self.deleteOldContainer(container_name)
                self.pullImage(self.storage[container_name]['image'])
                self.runContainer(self.storage[container_name]['image'], container_name, self.storage[container_name]['network'], self.storage[container_name]['command'])
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

    def runContainer(self, image, name, network, command):
        container = dHelper.runContainer(self.dockerClient, image, name, network=network, command=command)
        self.logger.info('Container %s is running.' % name)
        return container

    def main(self):
        migrateThr = threading.Thread(target=self.listenManagerMsg, args=())
        migrateThr.setDaemon(True)

        notMigrateThr = threading.Thread(target=self.listenWorkerMessage, args=())
        notMigrateThr.setDaemon(True)

        migrateThr.start()
        notMigrateThr.start()

    def requestJoinSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/SwarmLMGM/worker/requestJoin'
        requests.post(url=url, data=self.hostname)

    def requestLeaveSwarm(self):
        url = 'http://' + self.manager_addr + ':5000/SwarmLMGM/worker/requestLeave'
        requests.post(url=url, data=self.hostname)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Manager IP address.')
    args = parser.parse_args()
    manager_addr = args.address
    worker = Worker(manager_addr)

    while True:
        join = input('Would you like to join Swarm environment? (y/n)')
        if join == 'y':
            worker.requestJoinSwarm()
            worker.main()
            while True:
                leave = input('Press \'q\' to leave Swarm environment.')
                if leave == 'q':
                    worker.requestLeaveSwarm()
                    break