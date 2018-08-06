#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import requests
import random
import time
import utl
import json


class StressClient(object):
    class Task:
        def __init__(self, container_name, image, cpu_count, mem_limit, volume=None, command="", node="", cpuset_cpus="", deadline=0):
            self.container_name = container_name
            self.image = image
            self.command = command
            self.cpu_count = cpu_count
            self.mem_limit = mem_limit
            self.node = node
            self.volume = volume
            self.cpuset_cpus = cpuset_cpus
            self.deadline = deadline

        def generate_task(self):
            task = {"container_name": self.container_name,
                    "node": self.node,
                    "image": self.image,
                    "detach": True,
                    "command": self.command,
                    "cpu_count": self.cpu_count,
                    "cpuset_cpus": self.cpuset_cpus,
                    "mem_limit": self.mem_limit,
                    "ports": {},
                    "volumes": self.volume,
                    "environment": {},
                    "deadline": self.deadline,
                    "status": "Ready"}
            return task

    __metaclass__ = ABCMeta

    def init_fields(self):
        with open('StressClientInfo.json', 'r') as f:
            data = json.load(f)
        print(data)
        self.subnet = data['subnet']
        self.image_name = data['image_name']
        self.task_count = data['task_count']
        self.task_cores = data['task_cores']
        self.task_mem = data['task_mem']
        self.time_interval = data['time_interval']

        with open('ClientInit.json') as f:
            data = json.load(f)
        self.fe_addr = data['front_end_addr']
        self.fe_port = data['front_end_port']

        self.logger = utl.doLog('StressClientLogger', 'StressClient.log')

    def generate_job(self, job_name):
        with open('SampleJob.json', 'r') as f:
            job = json.load(f)
        job['job_name'] = job_name
        job['job_info']['network']['subnet'] = self.subnet
        job['job_info']['network']['name'] += '_' + job_name

        core_choices = [str(i) for i in range(48)]
        for i in range(self.task_count):
            task_name = job_name + '_task' + str(i+1)

            # For testing switch scheduler
            node_choice = ['testswarmw1', 'testswarmw2']
            node = node_choice[random.randint(0, 1)]
            cores = []
            for i in range(4):
                _core = core_choices[random.randint(0, len(core_choices)-1)]
                cores.append(_core)
                core_choices.remove(_core)

            cpuset_cpus = ','.join(cores)

            image = self.image_name
            cpu_count = self.task_cores
            mem_limit = str(self.task_mem) + 'm'

            host_dir = '/nfs/RESTfulSwarm/%s/%s' % (job_name, task_name)
            volume = {'/home/volume': {'bind': host_dir, 'mode': 'rw'}}
            task = self.Task(task_name, image, cpu_count, mem_limit, volume=volume, node=node, cpuset_cpus=cpuset_cpus, deadline=random.randint(0, 20))
            task = task.generate_task()
            job['job_info']['tasks'].update({task_name: task})
        return job

    @abstractmethod
    def feed_func(self, time_stamp):
        return 0

    def feed_jobs(self, session_id):
        total_jobs = 0

        max_time = 5 * self.time_interval
        start_time = time.time()

        def feed(_session):
            nonlocal total_jobs
            time_index = 0
            while time_index <= max_time:
                job_count = self.feed_func(time_index)
                self.logger.info('Time Index----%d----Job#----%d' % (time_index, job_count))
                total_jobs += job_count
                for i in range(job_count):
                    job_name = 'job' + str(int(time.time() * 1000)) + '-' + _session
                    self.newJob(self.generate_job(job_name))
                time.sleep(self.time_interval)
                time_index += self.time_interval

        feed(session_id)
        self.logger.info('Total jobs #: %d' % total_jobs)
        elapsed_time = time.time() - start_time
        self.logger.info('Elapsed time: %f' % elapsed_time)

        # time.sleep(5)
        # session_id = str(int(time.time()))
        # # Switch scheduler
        # url = 'http://%s:%s/RESTfulSwarm/FE/switchScheduler/no-scheduler' % (self.fe_addr, self.fe_port)
        # print(requests.get(url=url).content)
        # print('------------------------------------------------')
        # print('Switch Scheduler to No Scheduler')
        # print('------------------------------------------------')
        #
        # time.sleep(5)
        # feed(session_id)

    def newJob(self, data):
        url = 'http://%s:%s/RESTfulSwarm/FE/requestNewJob' % (self.fe_addr, self.fe_port)
        print(requests.post(url=url, json=data).content)