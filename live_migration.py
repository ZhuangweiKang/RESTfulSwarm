#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import utl
import time
import random
import json
import docker_api as docker
from Messenger import Messenger


class LiveMigration:
    def __init__(self, image=None, name=None, network=None, logger=None, docker_client=None, storage=None):
        self.__image = image
        self.__name = name
        self.__logger = logger
        self.__docker_client = docker_client
        self.__messenger = None
        self.__network = network
        self.__storage = storage

    def send_image_info(self):
        self.__messenger.send(prompt='image', content=self.__image)

    def send_spawn_cmd(self, cmd):
        self.__logger.info('Send init command to destination node: %s' % cmd)
        self.__messenger.send(prompt='command', content=cmd)

    def send_container_detail(self, container_detail):
        self.__logger.info('Send container details to destination node.')
        self.__messenger.send(prompt='container_detail', content=json.dumps(container_detail))

    def dump_container(self):
        checkpoint_name = self.__name + '_' + str(random.randint(1, 1000))
        tar_name = checkpoint_name + '.tar'
        docker.checkpoint(checkpoint_name, docker.get_container_id(self.__docker_client, self.__name))
        return checkpoint_name, tar_name

    def tar_image(self, checkpoint_name, tar_name):
        utl.tar_files(tar_name, docker.get_container_id(self.__docker_client, self.__name), checkpoint_name)
        self.__logger.info('Tar dumped image files.')

    def transfer_tar(self, checkpoint_name, dst_address):
        file_name = checkpoint_name + '.tar'
        port = 3300
        utl.transfer_file(file_name, dst_address, port, self.__logger)

    def commit_con(self, name, image_name, repository):
        return docker.commit_container(self.__docker_client, name, repository, image_name)

    def migrate(self, dst_address, port='3200', cmd=None, container_detail=None):
        self.__messenger = Messenger(messenger_type='C/S', address=dst_address, port=port)
        checkpoint_name, tar_name = self.dump_container()

        self.commit_con(self.__name, self.__image, self.__image)
        self.__logger.info('Container has been committed.')
        self.send_image_info()
        self.send_spawn_cmd(cmd)
        self.send_container_detail(container_detail)

        self.tar_image(checkpoint_name, tar_name)
        self.transfer_tar(checkpoint_name, dst_address)

    def recv_image_info(self):
        msg = self.__messenger.receive('Ack')
        image = msg.split()[1]
        docker.pull_image(self.__docker_client, image)
        return image

    def recv_spawn_cmd(self):
        while True:
            msg = self.__messenger.receive('Ack')
            if msg.split()[0] == 'command':
                try:
                    command = msg.split()[1]
                    self.__logger.info('Received init command: %s ' % command)
                    return msg.split()[1]
                except IndexError as ex:
                    self.__logger.error(ex)

    def recv_container_detail(self):
        while True:
            msg = self.__messenger.receive('Ack')
            if msg.split()[0] == 'container_detail':
                try:
                    detail = json.loads(' '.join(msg.split()[1:]))
                    self.__logger.info('Received container details.')
                    return detail
                except IndexError as ex:
                    self.__logger.error(ex)

    def recv_tar(self):
        return utl.recv_file(self.__logger)

    def untar_checkpoint(self, file_name):
        utl.untar_file(file_name)
        self.__logger.info('Checkpoint has been untared...')

    def restore_container(self, checkpoint, new_container_name, new_image, network, command=None):
        checkpoint_dir = '/var/lib/docker/tmp'

        # create the new container using base image
        docker.create_container(self.__docker_client, new_image, new_container_name, network, command)

        docker.restore(new_container_name, checkpoint_dir, checkpoint)
        self.__logger.info('Container has been restored...')

    def not_migrate(self, port='3200'):
        self.__messenger = Messenger(messenger_type='C/S', port=port)
        while True:
            new_image = self.recv_image_info()
            command = self.recv_spawn_cmd()
            detail = self.recv_container_detail()
            tar_file = self.recv_tar()
            time.sleep(1)
            checkpoint = tar_file.split('.')[0]
            container_name = checkpoint.split('_')[0] + '_' + checkpoint.split('_')[1]
            if tar_file is not None:
                self.untar_checkpoint(file_name=tar_file)
                self.restore_container(checkpoint, container_name, new_image, detail['network'], command=command)
                self.__storage.update({container_name: detail})

    def menue(self, cmd=None):
        migrate = raw_input('Would you like to migrate your container?(y/n) ')
        if migrate == 'y':
            dst_addr = raw_input('Please enter the IP address of destination node: ')
            self.migrate(dst_addr, cmd=cmd)
            self.__logger.info('Your container has been migrated. You can restore it on the destination node.')
        else:
            self.__logger.info('Your container is running. Other container might migrate to this node.')
            self.not_migrate()
