#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
import traceback
import multiprocessing
import threading
import requests
import json
import argparse
import random
import math
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from live_migration import LiveMigration
import docker_api as docker
from Messenger import Messenger
import utl
import SystemConstants


class Worker:
    def __init__(self, gm_address, worker_address, dis_address, task_monitor_frequency):
        self.__logger = utl.get_logger('WorkerLogger', 'worker.log')
        self.__messenger = Messenger(messenger_type='Pub/Sub', address=gm_address, port=SystemConstants.GM_PUB_PORT)
        self.__docker_client = docker.set_client()
        self.__gm_address = gm_address
        self.__host_address = worker_address
        self.__hostname = utl.get_hostname()
        self.__messenger.subscribe_topic(self.__host_address)

        # local storage
        # format: {$container : $containerInfo}
        self.storage = {}

        self.dis_address = dis_address
        self.task_monitor_frequency = task_monitor_frequency

    def monitor(self, dis_address, frequency=0.1):
        client = docker.set_client()
        time.sleep(frequency)
        cs_messenger = Messenger(messenger_type='C/S', address=dis_address, port=SystemConstants.DISCOVERY_PORT)
        time_end = math.floor(time.time())
        deployed_tasks = []
        while True:
            try:
                time_start = math.ceil(time.time())
                events = client.events(since=time_end,
                                       until=time_start,
                                       filters={'type': 'container',
                                                'event': 'die'},
                                       decode=True)
                time_end = math.floor(time.time())

                msgs = []
                for event in events:
                    if event['Actor']['Attributes']['name'] in self.storage.keys() and \
                            event['Actor']['Attributes']['name'] not in deployed_tasks:
                        msg = self.__hostname + ' ' + event['Actor']['Attributes']['name']
                        deployed_tasks.append(event['Actor']['Attributes']['name'])
                        msgs.append(msg)

                events.close()

                # 去重
                msgs = list(set(msgs))

                if len(msgs) != 0:
                    msgs = ','.join(msgs)
                    # Notify discovery block to update MongoDB
                    self.__logger.info('Discovery: %s' % msgs)
                    cs_messenger.send(content=msgs)

                time.sleep(frequency)

            except Exception as ex:
                traceback.print_exc(file=sys.stdout)
                self.__logger.error(ex)

    def listen_manager_msg(self):
        while True:
            try:
                msg = self.__messenger.subscribe()
                msg = msg.split()[1:]
                msg_type = msg[0]
                if msg_type == 'join':
                    remote_address = msg[1]
                    join_token = msg[2]
                    self.join_swarm(remote_address, join_token)
                elif msg_type == 'ID':
                    worker_id = msg[1]
                    self.__hostname = worker_id
                    self.__messenger.subscribe_topic(self.__hostname)
                elif msg_type == 'checkpoints':
                    data = json.loads(' '.join(msg[1:]))
                    threads = []
                    for i in range(0, len(data)):
                        checkpoint_name = data[i] + '_' + str(random.randint(1, 1000))
                        container_id = docker.get_container_id(self.__docker_client, data[i])
                        thr = threading.Thread(target=docker.checkpoint, args=(checkpoint_name, container_id, True,))
                        thr.setDaemon(True)
                        threads.append(thr)
                    [thr.start() for thr in threads]
                elif msg_type == 'migrate':
                    info = json.loads(' '.join(msg[1:]))
                    dst = info['dst']
                    container = info['container']
                    container_info = info['info']
                    container_info['node'] = self.__hostname
                    try:
                        temp_container = self.storage[container]
                        del self.storage[container]
                        try:
                            lm_controller = LiveMigration(image=temp_container['image'], name=container,
                                                          network=temp_container['network'], logger=self.__logger,
                                                          docker_client=self.__docker_client)
                            lm_controller.migrate(dst_address=dst, cmd=temp_container['command'],
                                                  container_detail=container_info)
                        except Exception as ex:
                            self.__logger.error(ex)
                            self.storage.update({container: temp_container})
                    except Exception as ex:
                        self.__logger.error(ex)
                elif msg_type == 'new_container':
                    info = json.loads(' '.join(msg[1:]))
                    container_name = info['container_name']
                    del info['node']
                    self.storage.update({container_name: info})
                    job_name = container_name.split('_')[0]
                    volume_dir = '/nfs/RESTfulSwarm/%s/%s' % (job_name, container_name)
                    os.mkdir(path=volume_dir)
                    self.run_container(self.storage[container_name])
                elif msg_type == 'update':
                    new_info = json.loads(' '.join(msg[1:]))
                    container_name = new_info['container_name']
                    cpuset_cpus = new_info['cpuset_cpus']
                    mem_limit = new_info['mem_limit']
                    docker.update_container(self.__docker_client, container_name=container_name,
                                            cpuset_cpus=cpuset_cpus, mem_limit=mem_limit)
                    self.__logger.info('Updated cpuset_cpus to %s, mem_limits to %s' % (cpuset_cpus, mem_limit))
                elif msg_type == 'leave':
                    docker.leave_swarm(self.__docker_client)
                    self.__logger.info('Leave Swarm environment.')
            except Exception as ex:
                self.__logger.error(ex)

    def listen_worker_message(self):
        lm_controller = LiveMigration(logger=self.__logger, docker_client=self.__docker_client, storage=self.storage)
        lm_controller.not_migrate(SystemConstants.WORKER_PORT)

    def join_swarm(self, remote_address, join_token):
        docker.join_swarm(self.__docker_client, join_token, remote_address)
        self.__logger.info('Worker node join the Swarm environment.')

    def delete_old_container(self, name):
        if docker.check_container(self.__docker_client, name):
            self.__logger.info('Old container %s exists, deleting old container.' % name)
            container = docker.get_container(self.__docker_client, name)
            docker.delete_container(container)

    def pull_image(self, image):
        if docker.check_image(self.__docker_client, image) is False:
            self.__logger.info('Image doesn\'t exist, pulling image.')
            docker.pull_image(self.__docker_client, image)
        else:
            self.__logger.info('Image already exists.')

    def run_container(self, container_info):
        container_name = container_info['container_name']
        image_name = container_info['image']
        network = container_info['network']
        command = container_info['command']
        cpuset_cpus = container_info['cpuset_cpus']
        mem_limit = container_info['mem_limit']
        detach = container_info['detach']
        ports = container_info['ports']
        volumes = container_info['volumes']
        environment = container_info['environment']
        container = docker.run_container(self.__docker_client,
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
        self.__logger.info('Container %s is running.' % container_name)
        return container

    def controller(self):
        manager_monitor_thr = threading.Thread(target=self.listen_manager_msg, args=())
        peer_monitor_thr = threading.Thread(target=self.listen_worker_message, args=())
        container_monitor_thr = threading.Thread(target=self.monitor,
                                                 args=(self.dis_address, self.task_monitor_frequency,))
        container_monitor_thr.daemon = True
        manager_monitor_thr.daemon = True
        peer_monitor_thr.daemon = True

        container_monitor_thr.start()
        manager_monitor_thr.start()
        peer_monitor_thr.start()

    def request_join_swarm(self):
        url = 'http://' + self.__gm_address + ':5000/RESTfulSwarm/GM/request_join'
        json_info = {
            'hostname': self.__hostname,
            'address': self.__host_address,
            'CPUs': utl.get_total_cores(),
            'MemFree': utl.get_total_mem()
        }

        response = requests.post(url=url, json=json_info)

        # configure nfs
        if response.status_code == 200:
            cmd = 'sudo umount %s:/var/nfs/RESTfulSwarm' % self.__gm_address
            os.system(cmd)

            # mount to the directory on nfs host server(GlobalManager)
            cmd = 'sudo mount %s:/var/nfs/RESTfulSwarm /nfs/RESTfulSwarm' % self.__gm_address
            os.system(cmd)

    def request_leave_swarm(self):
        url = 'http://' + self.__gm_address + ':5000/RESTfulSwarm/GM/request_leave'
        requests.post(url=url, json={'hostname': self.__hostname})

    @staticmethod
    def main(frequency):
        os.chdir('/home/%s/RESTfulSwarm/Worker' % utl.get_username())

        with open('../ActorsInfo.json') as f:
            data = json.load(f)
        gm_address = data['GM']['address']
        worker_address = utl.get_local_address()
        dis_address = data['DC']['address']
        frequency = frequency

        worker = Worker(gm_address, worker_address, dis_address, frequency)

        worker.controller()
        worker.request_join_swarm()

        while True:
            pass


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--frequency', type=int, default=5, help='Frequency of sending data to Discovery.')
    args = parser.parse_args()
    frequency = args.frequency

    pro = multiprocessing.Process(
        name='Worker',
        target=Worker.main,
        args=(frequency, )
    )
    pro.daemon = True
    pro.start()
    pro.join()