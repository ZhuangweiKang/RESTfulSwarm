#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import requests
import argparse
import random
import pandas as pd
import json

fe_addr = None
fe_port = None


class Task:
    def __init__(self, container_name, image, cpu_count, mem_limit, command=""):
        self.container_name = container_name
        self.image = image
        self.command = command
        self.cpu_count = cpu_count
        self.mem_limit = mem_limit

    def get_task(self):
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


class StressClient:
    def __init__(self, subnet, image_name, workload):
        self.subnet = subnet
        self.image_name = image_name
        self.workload = workload

    def generate_job(self, job_name, tasks_count):
        with open('SampleJob.json', 'r') as f:
            job = json.load(f)
        job['job_name'] = job_name
        job['job_info']['network']['subnet'] = self.subnet
        for i in range(tasks_count):
            task_name = job_name + '_task' + str(i+1)
            image = self.image_name
            cpu_count = random.randint(2, 6)
            mem_limit = str(random.randint(10, 50)) + 'm'
            task = Task(task_name, image, cpu_count, mem_limit)
            task = task.get_task()
            job['job_info']['tasks'].update({task_name: task})
        return job

    def steady(self):
        jobs_data = {'time': [], 'job_count': []}
        time = 0
        while time < 200:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(3)
            time += 10
        df = pd.DataFrame(jobs_data, columns=['time', 'job_count'])
        df.to_csv('steady.csv')

    def bursty(self):
        jobs_data = {'time': [], 'job_count': []}
        time = 0
        while time < 40:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(2)
            time += 10
        while time < 60:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(0.9*time-30)
            time += 10
        while time < 80:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(-0.9*time+74)
            time += 10
        while time < 120:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(2)
            time += 10
        while time < 140:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(0.9*time-106)
            time += 10
        while time < 160:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(-0.9*time+146)
            time += 10
        while time < 200:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(2)
            time += 10
        df = pd.DataFrame(jobs_data, columns=['time', 'job_count'])
        df.to_csv('bursty.csv')

    def incremental(self):
        jobs_data = {'time': [], 'job_count': []}
        time = 0
        while time < 200:
            jobs_data['time'].append(time)
            count = random.randint(0.1*time, 0.1*(time+20))
            jobs_data['job_count'].append(count)
            time += 10
        df = pd.DataFrame(jobs_data, columns=['time', 'job_count'])
        df.to_csv('incremental.csv')

    def random(self):
        jobs_data = {'time': [], 'job_count': []}
        time = 0
        while time < 200:
            jobs_data['time'].append(time)
            jobs_data['job_count'].append(random.randint(1, 20))
            time += 10
        df = pd.DataFrame(jobs_data, columns=['time', 'job_count'])
        df.to_csv('random.csv')


def newJob(data):
    url = 'http://%s:%s/RESTfulSwarm/FE/requestNewJob' % (fe_addr, fe_port)
    print(requests.post(url=url, json=data).content)


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    # parser.add_argument('-p', '--port', type=str, default='5000', help='Front end node port number.')
    # args = parser.parse_args()
    # fe_addr = args.address
    # fe_port = args.port

    with open('ClientInit.json') as f:
        data = json.load(f)
    fe_addr = data['front_end_addr']
    fe_port = data['front_end_port']

    while True:
        try:
            json_path = input('Job Info Json file path:')
            with open(json_path, 'r') as f:
                data = json.load(f)
            newJob(data)
        except ValueError as er:
            print(er)