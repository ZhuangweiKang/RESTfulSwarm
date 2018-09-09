#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import traceback
import random
import time
import json
import requests

from abc import ABCMeta, abstractmethod
import utl
import SystemConstants


class StressClient(object):
    class Task(object):
        # TODO(deadline): Implement deadline for priority scheduler
        def __init__(self, container_name, image, req_cores, mem_limit, volume=None, command="", node="",
                     cpuset_cpus="", deadline=0, ports=None):
            self.container_name = container_name
            self.image = image
            self.command = command
            self.volume = volume
            self.req_cores = req_cores
            self.mem_limit = mem_limit
            self.cpuset_cpus = cpuset_cpus
            self.ports = ports

            self.node = node
            self.deadline = deadline

        def generate_task(self):
            task = {"container_name": self.container_name,
                    "node": self.node,
                    "image": self.image,
                    "detach": True,
                    "command": self.command,
                    "req_cores": self.req_cores,
                    "cpuset_cpus": self.cpuset_cpus,
                    "mem_limit": self.mem_limit,
                    "ports": self.ports,
                    "volumes": self.volume,
                    "environment": {},
                    "deadline": self.deadline,
                    "status": "Ready"}
            return task

    __metaclass__ = ABCMeta

    def __init__(self):
        with open('StressClientInfo.json') as f:
            data = json.load(f)
        self.subnet = data['subnet']
        self.image_name = data['image_name']
        self.task_count = data['task_count']
        self.task_cores = data['task_cores']
        self.task_mem = data['task_mem']
        self.time_interval = data['time_interval']

        with open('../ActorsInfo.json') as f:
            data = json.load(f)

        self.fe_address = data['FE']['address']

        self.logger = utl.get_logger('StressClientLogger', 'StressClient.log')

    def generate_job(self, job_name):
        with open('SampleJob.json', 'r') as f:
            job = json.load(f)

        job['job_name'] = job_name
        job['job_info']['network']['subnet'] = self.subnet
        job['job_info']['network']['name'] += '_' + job_name

        # core_choices = [str(i) for i in range(48)]
        for i in range(self.task_count):
            task_name = job_name + '_task' + str(i+1)

            node = ""
            cpuset_cpus = ""

            # For testing NodeScheduler
            # node_choice = ['testswarmw1', 'testswarmw2']
            # node = node_choice[random.randint(0, 1)]
            # cores = []
            # for i in range(4):
            #     _core = core_choices[random.randint(0, len(core_choices)-1)]
            #     cores.append(_core)
            #     core_choices.remove(_core)
            #
            # cpuset_cpus = ','.join(cores)

            image = self.image_name
            req_cores = self.task_cores
            mem_limit = str(self.task_mem) + 'm'

            # mount directory in container to /nfs/RESTfulSwarm/ directory in host machine
            host_dir = '/nfs/RESTfulSwarm/%s/%s' % (job_name, task_name)
            volume = {host_dir: {'bind': '/home/mnt/', 'mode': 'rw'}}

            task = self.Task(task_name, image, req_cores, mem_limit, volume=volume, node=node, cpuset_cpus=cpuset_cpus,
                             deadline=random.randint(0, 20))
            task = task.generate_task()
            job['job_info']['tasks'].update({task_name: task})
        return job

    @abstractmethod
    def feed_func(self, time_stamp):
        return 0

    def feed_jobs(self, session_id):
        jobs_count = 0

        max_turns = 5 * self.time_interval
        start_time = time.time()

        def feed(session):
            nonlocal jobs_count
            time_index = 0
            while time_index <= max_turns:
                job_count = self.feed_func(time_index)
                self.logger.info('Time Index----%d  /   Job#----%d' % (time_index, job_count))
                jobs_count += job_count
                for i in range(job_count):
                    job_name = 'job' + str(int(time.time() * 1000)) + '-' + session
                    self.new_job(self.generate_job(job_name))
                time.sleep(self.time_interval)
                time_index += self.time_interval

        feed(session_id)
        self.logger.info('Total jobs #: %d' % jobs_count)
        elapsed_time = time.time() - start_time
        self.logger.info('Elapsed time: %f' % elapsed_time)

        time.sleep(5)

        # while True:
        #     choice = input('Would you like to switch to new scheduler? (y/n)')
        #     if choice == 'y':
        #         print('1. Best Fit\n2. First Fit\n3. Best Fit Decreasing\n4. First Fit Decreasing')
        #         while True:
        #             try:
        #                 choice = int(input('What\'s your choice? '))
        #             except ValueError as er:
        #                 print(er)
        #             else:
        #                 break
        #         session_id = str(int(time.time()))
        #
        #         # Switch scheduler
        #         url = {
        #             1: 'http://%s:%s/RESTfulSwarm/FE/switch_scheduler/best-fit' %
        #                (self.fe_address, SystemConstants.FE_PORT),
        #             2: 'http://%s:%s/RESTfulSwarm/FE/switch_scheduler/first-fit' %
        #                (self.fe_address, SystemConstants.FE_PORT),
        #             3: 'http://%s:%s/RESTfulSwarm/FE/switch_scheduler/best-fit-decreasing' %
        #                (self.fe_address, SystemConstants.FE_PORT),
        #             4: 'http://%s:%s/RESTfulSwarm/FE/switch_scheduler/first-fit-decreasing' %
        #                (self.fe_address, SystemConstants.FE_PORT)
        #         }.get(choice, 1)
        #
        #         requests.get(url=url)
        #         print('------------------------------------------------')
        #         print('Switch Scheduler to new scheduler')
        #         print('------------------------------------------------')
        #
        #         time.sleep(5)
        #         feed(session_id)

    def new_job(self, data):
        url = 'http://%s:%s/RESTfulSwarm/FE/request_new_job' % (self.fe_address, SystemConstants.FE_PORT)
        requests.post(url=url, json=data)