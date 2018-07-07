#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import MongoDBHelper as mg
import ZMQHelper as zmq
import json
import argparse


class Discovery:
    def __init__(self, db_addr, db_port, discovery_port='4000'):
        db_client = mg.get_client(db_addr, db_port)
        self.db = mg.get_db(db_client, 'RESTfulSwarmDB')
        self.workers_info = mg.get_col(self.db, 'WorkersInfo')
        self.socket = zmq.csBind(discovery_port)

    def discovery(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            worker_host = msg.split()[0]
            msg = msg.split()[1]
            task_name = msg
            job_name = msg.split('_')[0]
            job_col = mg.get_col(self.db, job_name)

            # update job collection
            filter_key = 'job_info.tasks.%s.container_name' % task_name
            target_key = 'job_info.tasks.%s.status'
            mg.update_doc(job_col, filter_key, task_name, target_key, 'Down')

            # get the resource utilization of the 'Down' container
            job_info = mg.find_col(job_col)[0]
            cores = job_info['job_info']['tasks'][task_name]['cpuset_cpus']
            cores = cores.split(',')
            memory = job_info['job_info']['tasks'][task_name]['mem_limit']

            # update WorkersInfo collection
            # update cores info
            for core in cores:
                target_key = 'CPUs.%s' % core
                mg.update_doc(self.workers_info, 'hostname', worker_host, target_key, False)

            # update memory info
            worker_info = mg.filter_col(self.workers_info, 'hostname', worker_host)
            free_memory = worker_info['MemFree']
            memory = int(memory.split('m')[0])
            free_memory = int(free_memory.split('m')[0])
            updated_memory = memory + free_memory
            updated_memory = str(updated_memory) + 'm'
            mg.update_doc(self.workers_info, 'hostname', worker_host, 'MemFree', updated_memory)


if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-ma', 'mongo_addr', type=str, help='Mongodb server address.')
    parser.add_argument('-mp', 'mongo_port', type=str, default='27017', help='Mongodb server port.')
    args = parser.parse_args()
    mongo_addr = args.mongo_addr
    mongo_port = args.mongo_port
    '''
    with open('DiscoveryInit.json') as f:
        data = json.load(f)
    mongo_addr = data['mongo_addr']
    mongo_port = data['mongo_port']

    discovery = Discovery(mongo_addr, mongo_port)
    discovery.discovery()