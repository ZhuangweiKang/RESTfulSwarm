#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
import argparse
import utl
import json
import threading
from Scheduler import Scheduler
import ZMQHelper as zmq
import MongoDBHelper as mHelper


class JobManager:
    def __init__(self, gm_addr, gm_port, db, scheduler):
        self.gm_addr = gm_addr
        self.gm_port = gm_port
        self.db = db
        self.scheduler = scheduler
        self.workersInfoCol = self.db['WorkersInfo']

        # listening msg from FrontEnd
        self.socket = zmq.csBind(port='2990')

    # Initialize global manager(Swarm manager node)
    def init_gm(self):
        url = 'http://%s:%s/RESTfulSwarm/GM/init' % (self.gm_addr, self.gm_port)
        print(requests.get(url=url).content)

    # Create a single task(container) using a Json file
    def new_task(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/newtask' % (self.gm_addr, self.gm_port)
        print(requests.post(url=url, json=data).content)

    # Migrate a container
    # !!!Note: Assuming destination node always has enough cores to hold the migrated container
    def doMigrate(self, data):
        dest_node_name = data['to'].split('@')[0]
        dest_node_info = self.scheduler.get_node_info(dest_node_name)
        if dest_node_info is None:
            print('Migration failed, because node %s is not in Swarm environment.')
            return False
        else:
            # update node name
            job_name = data['job']
            job_col = self.db[job_name]
            container = data['container']

            filter_key = 'job_info.tasks.%s.container_name' % container
            target_key = 'job_info.tasks.%s.node' % container
            mHelper.update_doc(job_col, filter_key=filter_key, filter_value=container,
                               target_key=target_key, target_value=dest_node_name)

            # get cores id from source node
            job_info = list(job_col.find({}))[0]
            cores = job_info['job_info']['tasks'][container]['cpuset_cpus']
            cores = cores.split(',')

            # get memory info from source node
            mem_limits = job_info['job_info']['tasks'][container]['mem_limit']
            mem_limits = utl.memory_size_translator(mem_limits)

            # get free cores from destination node
            free_cores = []
            for core in dest_node_info['CPUs'].keys():
                if len(free_cores) == len(cores):
                    break
                if dest_node_info['CPUs'][core] is False:
                    free_cores.append(core)

            # update cpuset_cpus in job info collection
            target_key = 'job_info.tasks.%s.cpuset_cpus' % container
            mHelper.update_doc(job_col, filter_key=filter_key, filter_value=container,
                               target_key=target_key, target_value=','.join(free_cores))

            # release cores in source node
            src_node_name = scheduler.find_container(data['container'])
            for core in cores:
                target_key = 'CPUs.%s' % core
                mHelper.update_doc(self.workersInfoCol, 'hostname', src_node_name, target_key, False)

            # mark cores in destination node as busy
            for core in free_cores:
                target_key = 'CPUs.%s' % core
                mHelper.update_doc(self.workersInfoCol, 'hostname', dest_node_name, target_key, True)

            # get free memory from both source node and destination node
            src_worker_info = list(self.workersInfoCol.find({'hostname': src_node_name}))[0]
            src_free_mem = src_worker_info['MemFree']
            new_free_mem = dest_node_info['MemFree']

            # update memory field in both source node and destination node
            update_src_mem = str(utl.memory_size_translator(src_free_mem) + utl.memory_size_translator(mem_limits)) + 'm'
            update_dest_mem = str(utl.memory_size_translator(new_free_mem) - utl.memory_size_translator(mem_limits)) + 'm'

            mHelper.update_doc(self.workersInfoCol, 'hostname', src_node_name, 'MemFree', update_src_mem)
            mHelper.update_doc(self.workersInfoCol, 'hostname', dest_node_name, 'MemFree', update_dest_mem)

            # pre-process migration json file
            data.update({'from': data['from'].split('@')[1]})
            data.update({'to': data['to'].split('@')[1]})

            url = 'http://%s:%s/RESTfulSwarm/GM/requestMigrate' % (self.gm_addr, self.gm_port)
            print(requests.post(url=url, json=data).content)

            return True

    # Update container resources(cpu & mem)
    def updateContainer(self, data):
        # Update Job Info collection
        new_cpu = data['cpuset_cpus']
        new_mem = data['mem_limits']
        node = data['node']
        container_name = data['container']
        job = data['job']

        job_col = self.db[job]
        filter_key = 'job_info.tasks.%s.container_name' % container_name
        filter_value = container_name

        # get current cpu info
        container_info = list(job_col.find({}))[0]
        current_cpu = container_info['job_info']['tasks'][container_name]['cpuset_cpus']

        # get current memory info
        current_mem = container_info['job_info']['tasks'][container_name]['mem_limit']

        # update cpu
        target_key = 'job_info.tasks.%s.cpuset_cpus' % container_name
        target_value = new_cpu
        mHelper.update_doc(job_col, filter_key, filter_value, target_key, target_value)

        # update memory
        target_key = 'job_info.tasks.%s.mem_limits' % container_name
        target_value = new_mem
        mHelper.update_doc(job_col, filter_key, filter_value, target_key, target_value)

        # ----------------------------------------------------------------------
        # Update WorkersInfo Collection
        current_cpu = current_cpu.split(',')
        new_cpu = new_cpu.split(',')
        # update cpu
        for core in current_cpu:
            key = 'CPUs.%s' % core
            mHelper.update_doc(self.workersInfoCol, 'hostname', node, key, False)

        for core in new_cpu:
            key = 'CPUs.%s' % core
            mHelper.update_doc(self.workersInfoCol, 'hostname', node, key, True)

        # update memory, and we assuming memory unit is always m
        current_mem = utl.memory_size_translator(current_mem)
        worker_info = list(self.workersInfoCol.find({'hostname': node}))[0]
        free_mem = utl.memory_size_translator(worker_info['MemFree'])
        current_mem = free_mem + current_mem
        new_mem = utl.memory_size_translator(new_mem)
        current_mem -= new_mem
        current_mem = str(current_mem) + 'm'
        mHelper.update_doc(self.workersInfoCol, 'hostname', node, 'MemFree', current_mem)

        url = 'http://%s:%s/RESTfulSwarm/GM/requestUpdateContainer' % (self.gm_addr, self.gm_port)
        print(requests.post(url=url, json=data).content)

    # Leave Swarm
    def leaveSwarm(self, hostname):
        col_names = mHelper.get_all_cols(self.db)
        cols = []
        # get collection cursor objs
        for col_name in col_names:
            cols.append(mHelper.get_col(db, col_name))
        mHelper.update_tasks(cols, hostname)

        # remove the node(document) from WorkersInfo collection
        mHelper.delete_document(self.workersInfoCol, 'hostname', hostname)

        data = {'hostname': hostname}
        url = 'http://%s:%s/RESTfulSwarm/GM/requestLeave' % (self.gm_addr, self.gm_port)
        print(requests.post(url=url, json=data).content)

    def dumpContainer(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/checkpointCons' % (self.gm_addr, self.gm_port)
        print(requests.post(url=url, json=data).content)

    def newJobNotify(self):
        while True:
            msg = self.socket.recv_string()
            self.socket.send_string('Ack')
            msg = msg.split()

            # read db, parse job resources request
            job_name = msg[1]
            job_col = mHelper.get_col(self.db, job_name)
            col_data = mHelper.find_col(job_col)[0]

            # core request format: {$task_name: $core}}
            core_requests = {}
            # memory request format: {$task_name: $mem}
            mem_requests = {}
            for task in col_data['job_info']['tasks'].items():
                core_requests.update({task[0]: task[1]['cpu_count']})
                core_requests.update({task[0]: task[1]['cpu_count']})
                mem_requests.update({task[0]: task[1]['mem_limit']})

            # !!! Assuming we have enough capacity to hold any job
            # check resources
            schedule = scheduler.schedule_resources(core_request=core_requests, mem_request=mem_requests)

            if schedule is not None:
                self.scheduler.update_job_info(job_name, schedule)

                # update WorkersInfo collection
                self.scheduler.update_workers_info(schedule)
                url = 'http://%s:%s/RESTfulSwarm/GM/requestNewJob' % (self.gm_addr, self.gm_port)

                print(requests.post(url=url, json=col_data).content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ga', '--gaddr', type=str, help='Global manager node address.')
    parser.add_argument('-gp', '--gport', type=str, default='3100', help='Global manager node port number.')
    parser.add_argument('-ma', '--maddr', type=str, help='MongoDB node address.')
    parser.add_argument('-mp', '--mport', type=int, default=27017, help='MongoDB port number.')
    args = parser.parse_args()
    gm_addr = args.gaddr
    gm_port = args.gport

    mongo_addr = args.maddr
    mongo_port = args.mport

    db_name = 'RESTfulSwarmDB'
    db_client = mHelper.get_client(mongo_addr, mongo_port)
    db = mHelper.get_db(db_client, db_name)
    scheduler = Scheduler.Scheduler(db, 'WorkersInfo')

    job_manager = JobManager(gm_addr=gm_addr, gm_port=gm_port, db=db, scheduler=scheduler)

    fe_notify_thr = threading.Thread(target=job_manager.newJobNotify, args=())
    fe_notify_thr.setDaemon(True)
    fe_notify_thr.start()

    while True:
        print('--------------RESTfulSwarm Menu--------------')
        print('1. Init Swarm')
        print('2. Create task(one container)')
        print('3. Check point a group containers')
        print('4. Migrate a container')
        print('5. Migrate a group of containers')
        print('6. Update Container')
        print('7. Leave Swarm')
        print('8. Describe Workers')
        print('9. Describe Manager')
        print('10. Exit')
        try:
            get_input = int(input('Please enter your choice: '))
            if get_input == 1:
                job_manager.init_gm()
            elif get_input == 2:
                json_path = input('Task Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                job_manager.new_task(data)
            elif get_input == 3:
                json_path = input('Checkpoint Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                job_manager.dumpContainer(data)
            elif get_input == 4:
                json_path = input('Migration Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                job_manager.doMigrate(data)
            elif get_input == 5:
                migrate_json = input('Group migration Json file: ')
                with open(migrate_json, 'r') as f:
                    data = json.load(f)
                for item in data:
                    job_manager.doMigrate(data)
            elif get_input == 6:
                json_path = input('New resource configuration Json file:')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                job_manager.updateContainer(data)
            elif get_input == 7:
                hostname = input('Node hostname: ')
                job_manager.leaveSwarm(hostname)
            elif get_input == 8:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/RESTfulSwarm/GM/%s/describeWorker' % (gm_addr, gm_port, hostname)
                print(requests.get(url=url).content.decode('utf-8'))
            elif get_input == 9:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/RESTfulSwarm/GM/%s/describeManager' % (gm_addr, gm_port, hostname)
                print(requests.get(url=url).content.decode('utf-8'))
            elif get_input == 10:
                print('Thanks for using RESTfulSwarmLM, bye.')
                break
        except ValueError as er:
            print(er)