#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import utl
import threading
import time
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

    def listenMsg(self):
        while True:
            msg = self.swarmSocket.recv()
            msg = msg.split()[1:]
            msg_type = msg[0]
            if msg_type == 'join':
                remote_addr = msg[1]
                join_token = msg[2]
                self.joinSwarm(remote_addr, join_token)
            elif msg_type == 'migrate':
                pass
            elif msg_type == 'new_container':
                pass
            elif msg_type == 'leave':
                pass

    def joinSwarm(self, remote_addr, join_token):
        dHelper.joinSwarm(self.dockerClient, join_token, remote_addr)
        self.logger.info('Worker node join the Swarm environment.')

    def recvContainerAddr(self):
        zmqHelper.addTopic(self.swarmInfoSocket, 'container-ip')
        msg = self.swarmInfoSocket.recv_string()
        msg = msg.split()[1]
        self.pub_con_addr = msg
        self.logger.info('Received container address')

    def deleteOldContainer(self):
        if dHelper.checkContainer(self.dockerClient, self.name) is True:
            container = dHelper.getContainer(self.dockerClient, self.name)
            dHelper.deleteContainer(container)
            self.logger.info('Old container exists, deleting old container.')

    def pullImage(self, image):
        if dHelper.checkImage(self.dockerClient, image) is False:
            dHelper.pullImage(self.dockerClient, image)
            self.logger.info('Image doesn\'t exist, building image.')

    def runContainer(self, pub_con_port='3000'):
        while self.pub_con_addr is None:
            pass
        command = 'python SubscribeData.py -a %s -p %s' % (self.pub_con_addr, pub_con_port)
        container = dHelper.runContainer(self.dockerClient, self.image, self.name, network=self.network, command=command)
        self.logger.info('Container is running.')
        return container

    def main(self):
        self.joinSwarm(self.pub_node_addr)
        self.recvContainerAddr()
        self.deleteOldContainer()
        self.pullImage(self.image)
        self.runContainer()
        cmd = 'python SubscribeData.py -a %s -p %s' % (self.pub_con_addr, '3000')
        self.lmController.menue(cmd=cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='The IP address of publisher.')
    args = parser.parse_args()
    address = args.address
    sub = Subscriber(name='subscriber', image='zhuangweikang/subscriber', pub_node_addr=address, network='kangNetwork')
    sub.main()