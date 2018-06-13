#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import utl
import time
import random
import json
import DockerHelper as dHelper
import ZMQHelper as zmqHelper


class LiveMigration:
    def __init__(self, image=None, name=None, network=None, logger=None, dockerClient=None, storage=None):
        self.image = image
        self.name = name
        self.logger = logger
        self.dockerClient = dockerClient
        self.socket = None
        self.network = network
        self.storage = storage

    def sendImageInfo(self):
        msg = 'image %s' % self.image
        self.socket.send_string(msg)
        self.socket.recv_string()

    def sendSpawnCmd(self, cmd):
        msg = 'command %s' % cmd
        self.logger.info('Send init command to destination node: %s' % cmd)
        self.socket.send_string(msg)
        self.socket.recv_string()

    def sendContainerDetail(self, container_detail):
        msg = 'container_detail %s' % json.dumps(container_detail)
        self.logger.info('Send container details to destination node.')
        self.socket.send_string(msg)
        self.socket.recv_string()

    def dumpContainer(self):
        checkpoint_name = self.name + '_' + str(random.randint(1, 1000))
        tarName = checkpoint_name + '.tar'
        dHelper.checkpoint(checkpoint_name, dHelper.getContainerID(self.dockerClient, self.name))
        return checkpoint_name, tarName

    def tarImage(self, checkpoint_name, tarName):
        utl.tarFiles(tarName, dHelper.getContainerID(self.dockerClient, self.name), checkpoint_name)
        self.logger.info('Tar dumped image files.')

    def transferTar(self, checkpoint_name, dst_addr):
        fileName = checkpoint_name + '.tar'
        port = 3300
        utl.transferFile(fileName, dst_addr, port, self.logger)

    def commitCon(self, name, imageName, repository):
        return dHelper.commitContainer(self.dockerClient, name, repository, imageName)

    def migrate(self, dst_addr, port='3200', cmd=None, container_detail=None):
        self.socket = zmqHelper.csConnect(dst_addr, port)
        checkpoint_name, tarName = self.dumpContainer()

        self.commitCon(self.name, self.image, self.image)
        self.logger.info('Container has been committed.')
        self.sendImageInfo()
        self.sendSpawnCmd(cmd)
        self.sendContainerDetail(container_detail)

        self.tarImage(checkpoint_name, tarName)
        self.transferTar(checkpoint_name, dst_addr)

    def recvImageInfo(self):
        msg = self.socket.recv_string()
        self.socket.send_string('Ack')
        image = msg.split()[1]
        dHelper.pullImage(self.dockerClient, image)
        return image

    def recvSpawnCmd(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            if msg.split()[0] == 'command':
                try:
                    command = msg.split()[1]
                    self.logger.info('Received init command: %s ' % command)
                    return msg.split()[1]
                except IndexError as ex:
                    return

    def recvContainerDetail(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            if msg.split()[0] == 'container_detail':
                try:
                    detail = json.loads(' '.join(msg.split()[1:]))
                    self.logger.info('Received container details.')
                    return detail
                except IndexError as ex:
                    return

    def recvTar(self):
        return utl.recvFile(self.logger)

    def unTarCheckpoint(self, fileName):
        utl.untarFile(fileName)
        self.logger.info('Checkpoint has been untared...')

    def restoreContainer(self, checkpoint, new_container_name, newImage, command=None):
        checkpoint_dir = '/var/lib/docker/tmp'

        # create the new container using base image
        dHelper.createContainer(self.dockerClient, newImage, new_container_name, self.network, command)

        dHelper.restore(new_container_name, checkpoint_dir, checkpoint)
        self.logger.info('Container has been restored...')

    def notMigrate(self, port='3200'):
        self.socket = zmqHelper.csBind(port)
        while True:
            newImage = self.recvImageInfo()
            command = self.recvSpawnCmd()
            detail = self.recvContainerDetail()
            tarFile = self.recvTar()
            time.sleep(1)
            checkpoint = tarFile.split('.')[0]
            container_name = checkpoint.split('_')[0]
            if tarFile is not None:
                self.unTarCheckpoint(fileName=tarFile)
                self.restoreContainer(checkpoint, container_name, newImage, command)
                self.storage.update({container_name: detail})

    def menue(self, cmd=None):
        migrate = raw_input('Would you like to migrate your container?(y/n) ')
        if migrate == 'y':
            dst_addr = raw_input('Please enter the IP address of destination node: ')
            self.migrate(dst_addr, cmd=cmd)
            self.logger.info('Your container has been migrated. You can restore it on the destination node.')
        else:
            self.logger.info('Your container is running. Other container might migrate to this node.')
            self.notMigrate()
