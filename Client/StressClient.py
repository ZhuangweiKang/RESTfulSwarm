#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import requests
import time
import json


class StressClient(object):
    class Task:
        def __init__(self, container_name, image, cpu_count, mem_limit, command=""):
            self.container_name = container_name
            self.image = image
            self.command = command
            self.cpu_count = cpu_count
            self.mem_limit = mem_limit

        def generate_task(self):
            task = {"container_name": self.container_name,
                    "node": "",
                    "image": self.image,
                    "detach": True,
                    "command": self.command,
                    "cpu_count": self.cpu_count,
                    "cpuset_cpus": "",
                    "mem_limit": self.mem_limit,
                    "ports": {},
                    "volumes": {},
                    "environment": {},
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

    def generate_job(self, job_name):
        with open('SampleJob.json', 'r') as f:
            job = json.load(f)
        job['job_name'] = job_name
        job['job_info']['network']['subnet'] = self.subnet
        for i in range(self.task_count):
            task_name = job_name + '_task' + str(i+1)
            image = self.image_name
            cpu_count = self.task_cores
            mem_limit = str(self.task_mem) + 'm'
            task = self.Task(task_name, image, cpu_count, mem_limit)
            task = task.generate_task()
            job['job_info']['tasks'].update({task_name: task})
        return job

    @abstractmethod
    def feed_func(self, time_stamp):
        return 0

    def feed_jobs(self):
        max_time = 20 * self.time_interval
        time_index = 0
        while time_index <= max_time:
            job_count = self.feed_func(time_index)
            for i in range(job_count):
                job_name = 'job' + str(int(time.time()))
                self.newJob(self.generate_job(job_name))
                # time.sleep(1)
            time.sleep(self.time_interval)
            time_index += self.time_interval

    def newJob(self, data):
        url = 'http://%s:%s/RESTfulSwarm/FE/requestNewJob' % (self.fe_addr, self.fe_port)
        print(requests.post(url=url, json=data).content)