#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import utl
import time
import random
import json
import docker_api as docker
import zmq_api as zmq


class LiveMigration:
    def __init__(self, image=None, name=None, network=None, logger=None, docker_client=None, storage=None):
        self.image = image
        self.name = name
        self.logger = logger
        self.docker_client = docker_client
        self.socket = None
        self.network = network
        self.storage = storage

    def send_image_info(self):
        msg = 'image %s' % self.image
        self.socket.send_string(msg)
        self.socket.recv_string()

    def send_spawn_cmd(self, cmd):
        msg = 'command %s' % cmd
        self.logger.info('Send init command to destination node: %s' % cmd)
        self.socket.send_string(msg)
        self.socket.recv_string()

    def send_container_detail(self, container_detail):
        msg = 'container_detail %s' % json.dumps(container_detail)
        self.logger.info('Send container details to destination node.')
        self.socket.send_string(msg)
        self.socket.recv_string()

    def dump_container(self):
        checkpoint_name = self.name + '_' + str(random.randint(1, 1000))
        tar_name = checkpoint_name + '.tar'
        docker.checkpoint(checkpoint_name, docker.get_container_id(self.docker_client, self.name))
        return checkpoint_name, tar_name

    def tar_image(self, checkpoint_name, tar_name):
        utl.tar_files(tar_name, docker.get_container_id(self.docker_client, self.name), checkpoint_name)
        self.logger.info('Tar dumped image files.')

    def transfer_tar(self, checkpoint_name, dst_address):
        file_name = checkpoint_name + '.tar'
        port = 3300
        utl.transfer_file(file_name, dst_address, port, self.logger)

    def commit_con(self, name, image_name, repository):
        return docker.commit_container(self.docker_client, name, repository, image_name)

    def migrate(self, dst_address, port='3200', cmd=None, container_detail=None):
        self.socket = zmq.cs_connect(dst_address, port)
        checkpoint_name, tar_name = self.dump_container()

        self.commit_con(self.name, self.image, self.image)
        self.logger.info('Container has been committed.')
        self.send_image_info()
        self.send_spawn_cmd(cmd)
        self.send_container_detail(container_detail)

        self.tar_image(checkpoint_name, tar_name)
        self.transfer_tar(checkpoint_name, dst_address)

    def recv_image_info(self):
        msg = self.socket.recv_string()
        self.socket.send_string('Ack')
        image = msg.split()[1]
        docker.pull_image(self.docker_client, image)
        return image

    def recv_spawn_cmd(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            if msg.split()[0] == 'command':
                try:
                    command = msg.split()[1]
                    self.logger.info('Received init command: %s ' % command)
                    return msg.split()[1]
                except IndexError as ex:
                    self.logger.error(ex)

    def recv_container_detail(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            if msg.split()[0] == 'container_detail':
                try:
                    detail = json.loads(' '.join(msg.split()[1:]))
                    self.logger.info('Received container details.')
                    return detail
                except IndexError as ex:
                    self.logger.error(ex)

    def recv_tar(self):
        return utl.recv_file(self.logger)

    def untar_checkpoint(self, file_name):
        utl.untar_file(file_name)
        self.logger.info('Checkpoint has been untared...')

    def restore_container(self, checkpoint, new_container_name, new_image, network, command=None):
        checkpoint_dir = '/var/lib/docker/tmp'

        # create the new container using base image
        docker.create_container(self.docker_client, new_image, new_container_name, network, command)

        docker.restore(new_container_name, checkpoint_dir, checkpoint)
        self.logger.info('Container has been restored...')

    def not_migrate(self, port='3200'):
        self.socket = zmq.cs_bind(port)
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
                self.storage.update({container_name: detail})

    def menue(self, cmd=None):
        migrate = raw_input('Would you like to migrate your container?(y/n) ')
        if migrate == 'y':
            dst_addr = raw_input('Please enter the IP address of destination node: ')
            self.migrate(dst_addr, cmd=cmd)
            self.logger.info('Your container has been migrated. You can restore it on the destination node.')
        else:
            self.logger.info('Your container is running. Other container might migrate to this node.')
            self.not_migrate()
