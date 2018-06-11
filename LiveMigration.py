#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import utl
import time
import random
import DockerHelper as dHelper
import ZMQHelper as zmqHelper


class LiveMigration:
    def __init__(self, image=None, name=None, network=None, logger=None, dockerClient=None):
        self.image = image
        self.name = name
        self.logger = logger
        self.dockerClient = dockerClient
        self.socket = None
        self.network = network

    def sendImageInfo(self):
        msg = 'image %s' % self.image
        self.socket.send_string(msg)
        self.socket.recv_string()

    def sendSpawnCmd(self, cmd):
        msg = 'command %s' % cmd
        self.logger.info('Send init command to destination node: %s' % cmd)
        self.socket.send_string(msg)
        self.socket.recv_string()

    def dumpContainer(self):
        checkpoint_name = 'checkpoint_' + str(random.randint(1, 100))
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

    def migrate(self, dst_addr, port='3200', cmd=None):
        self.socket = zmqHelper.csConnect(dst_addr, port)
        checkpoint_name, tarName = self.dumpContainer()

        self.commitCon(self.name, self.image, self.image)
        self.logger.info('Container has been committed.')
        self.sendImageInfo()
        self.sendSpawnCmd(cmd)

        self.tarImage(checkpoint_name, tarName)
        self.transferTar(checkpoint_name, dst_addr)

    def recvImageInfo(self):
        msg = self.socket.recv_string()
        self.socket.send_string('Ack')
        image = msg.split()[1]
        dHelper.pullImage(self.dockerClient, image)
        # self.logger.info('Image doesn\'t exist, building image.')
        return image

    def recvSpawnCmd(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            if msg.split()[0] == 'command':
                self.logger.info('Received init command: %s ' % msg.split()[1])
                return msg.split()[1]

    def recvTar(self):
        return utl.recvFile(self.logger)

    def unTarCheckpoint(self, fileName):
        fileName = fileName.strip('\00')
        utl.untarFile(fileName)
        self.logger.info('Checkpoint has been untared...')

    def restoreContainer(self, checkpoint, newImage, command=None):
        checkpoint_dir = '/var/lib/docker/tmp'
        newContainer = 'newContainerFrom' + checkpoint

        # create the new container using base image
        dHelper.createContainer(self.dockerClient, newImage, newContainer, self.network, command)

        dHelper.restore(newContainer, checkpoint_dir, checkpoint)
        self.logger.info('Container has been restored...')

    def notMigrate(self, port='3200'):
        self.socket = zmqHelper.csBind(port)
        while True:
            newImage = self.recvImageInfo()
            command = self.recvSpawnCmd()
            tarFile = self.recvTar()
            time.sleep(1)
            checkpoint = tarFile.split('.')[0]
            if tarFile is not None:
                self.unTarCheckpoint(fileName=tarFile)
                self.restoreContainer(checkpoint, newImage, command)

    def menue(self, cmd=None):
        migrate = raw_input('Would you like to migrate your container?(y/n) ')
        if migrate == 'y':
            dst_addr = raw_input('Please enter the IP address of destination node: ')
            self.migrate(dst_addr, cmd=cmd)
            self.logger.info('Your container has been migrated. You can restore it on the destination node.')
        else:
            self.logger.info('Your container is running. Other container might migrate to this node.')
            self.notMigrate()
