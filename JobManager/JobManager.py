#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import requests
import argparse
import utl
import time
import json
import threading

from Scheduler.BestFitScheduler import BestFitScheduler
from Scheduler.FirstFitScheduler import FirstFitScheduler
from Scheduler.BestFitDecreasingScheduler import BestFitDecreasingScheduler
from Scheduler.FirstFitDecreasingScheduler import FirstFitDecreasingScheduler
from Scheduler.NodeScheduler import NodeScheduler
import zmq_api as zmq
import mongodb_api as mg
import SystemConstants


class JobManager(object):
    def __init__(self, gm_address, db, scheduler, wait):
        self.gm_address = gm_address
        self.db = db
        self.scheduler = scheduler
        self.workersInfoCol = self.db['WorkersInfo']
        self.workers_resource_col = self.db['WorkersResourceInfo']
        self.wait = wait

        # listening msg from FrontEnd
        self.socket = zmq.cs_bind(port=SystemConstants.JM_PORT)

    # Initialize global manager(Swarm manager node)
    def init_gm(self):
        url = 'http://%s:%s/RESTfulSwarm/GM/init' % (self.gm_address, SystemConstants.GM_PORT)
        print(requests.get(url=url).content)

    # Create a single task(container) using a Json file
    def new_task(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/request_new_task' % (self.gm_address, SystemConstants.GM_PORT)
        print(requests.post(url=url, json=data).content)

    def container_migration(self, data):
        dest_node_name = (data['to'].split('@'))[0]
        dest_node_info = self.scheduler.get_node_info(dest_node_name)
        if dest_node_info is None:
            er = 'Migration failed, because node %s is not in Swarm environment.'
            raise Exception(er)
        else:
            # update node name
            job_name = data['job']
            job_col = self.db[job_name]
            container = data['container']

            filter_key = 'job_info.tasks.%s.container_name' % container
            target_key = 'job_info.tasks.%s.node' % container
            mg.update_doc(job_col, filter_key=filter_key, filter_value=container, target_key=target_key,
                          target_value=dest_node_name)

            # get cores id from source node
            job_info = list(job_col.find({}))[0]
            cores = job_info['job_info']['tasks'][container]['cpuset_cpus']
            cores = cores.split(',')

            # get memory info from source node
            mem_limit = job_info['job_info']['tasks'][container]['mem_limit']
            mem_limit = utl.memory_size_translator(mem_limit)

            # get free cores from destination node
            free_cores = []
            for core in dest_node_info['CPUs'].keys():
                if len(free_cores) == len(cores):
                    break
                if dest_node_info['CPUs'][core] is False:
                    free_cores.append(core)

            # update cpuset_cpus in job info collection
            target_key = 'job_info.tasks.%s.cpuset_cpus' % container
            mg.update_doc(job_col, filter_key=filter_key, filter_value=container, target_key=target_key,
                          target_value=','.join(free_cores))

            # release cores in source node
            src_node_name = self.scheduler.find_container(data['container'])
            for core in cores:
                target_key = 'CPUs.%s' % core
                mg.update_doc(self.workersInfoCol, 'hostname', src_node_name, target_key, False)

            # mark cores in destination node as busy
            for core in free_cores:
                target_key = 'CPUs.%s' % core
                mg.update_doc(self.workersInfoCol, 'hostname', dest_node_name, target_key, True)

            # get free memory from both source node and destination node
            src_worker_info = list(self.workersInfoCol.find({'hostname': src_node_name}))[0]
            src_free_mem = src_worker_info['MemFree']
            new_free_mem = dest_node_info['MemFree']

            # update memory field in both source node and destination node
            update_src_mem = str(utl.memory_size_translator(src_free_mem) + mem_limit) + 'm'
            update_dest_mem = str(utl.memory_size_translator(new_free_mem) - mem_limit) + 'm'

            mg.update_doc(self.workersInfoCol, 'hostname', src_node_name, 'MemFree', update_src_mem)
            mg.update_doc(self.workersInfoCol, 'hostname', dest_node_name, 'MemFree', update_dest_mem)

            # pre-process migration json file
            data.update({'from': data['from'].split('@')[1]})
            data.update({'to': data['to'].split('@')[1]})

            # update worker resource collection for both workers
            mg.update_workers_resource_col(workers_col=self.workersInfoCol, hostname=src_node_name,
                                           workers_resource_col=self.workers_resource_col)
            mg.update_workers_resource_col(workers_col=self.workersInfoCol, hostname=dest_node_name,
                                           workers_resource_col=self.workers_resource_col)
            return data

    # Migrate a container
    # !!!Note: Assuming destination node always has enough cores to hold the migrated container
    def do_migrate(self, data):
        try:
            data = self.container_migration(data)
            url = 'http://%s:%s/RESTfulSwarm/GM/request_migrate' % (self.gm_address, SystemConstants.GM_PORT)
            print(requests.post(url=url, json=data).content)
            return True
        except Exception as ex:
            print(ex)
            return False

    def do_group_migration(self, data):
        try:
            for index, item in enumerate(data[:]):
                new_item = self.container_migration(item)
                data[index].update(new_item)
            url = 'http://%s:%s/RESTfulSwarm/GM/request_group_migration' % (self.gm_address, SystemConstants.GM_PORT)
            print(requests.post(url=url, json=data).content)
            return True
        except Exception as ex:
            print(ex)
            return False

    # Update container resources(cpu & mem)
    def update_container(self, data):
        # Update Job Info collection
        new_cpu = data['cpuset_cpus']
        new_mem = data['mem_limit']
        node = data['node']
        container_name = data['container_name']
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
        mg.update_doc(job_col, filter_key, filter_value, target_key, target_value)

        # update memory
        target_key = 'job_info.tasks.%s.mem_limit' % container_name
        target_value = new_mem
        mg.update_doc(job_col, filter_key, filter_value, target_key, target_value)

        # ----------------------------------------------------------------------
        # Update WorkersInfo Collection
        current_cpu = current_cpu.split(',')
        new_cpu = new_cpu.split(',')
        # update cpu
        for core in current_cpu:
            key = 'CPUs.%s' % core
            mg.update_doc(self.workersInfoCol, 'hostname', node, key, False)

        for core in new_cpu:
            key = 'CPUs.%s' % core
            mg.update_doc(self.workersInfoCol, 'hostname', node, key, True)

        # update memory, and we assuming memory unit is always m
        current_mem = utl.memory_size_translator(current_mem)
        worker_info = list(self.workersInfoCol.find({'hostname': node}))[0]
        free_mem = utl.memory_size_translator(worker_info['MemFree'])
        current_mem = free_mem + current_mem
        new_mem = utl.memory_size_translator(new_mem)
        current_mem -= new_mem
        current_mem = str(current_mem) + 'm'
        mg.update_doc(self.workersInfoCol, 'hostname', node, 'MemFree', current_mem)

        # update worker resource Info collection
        mg.update_workers_resource_col(workers_col=self.workersInfoCol, hostname=node,
                                       workers_resource_col=self.workers_resource_col)

        url = 'http://%s:%s/RESTfulSwarm/GM/request_update_container' % (self.gm_address, SystemConstants.GM_PORT)
        print(requests.post(url=url, json=data).content)

    # Leave Swarm
    def leave_swarm(self, hostname):
        col_names = mg.get_all_cols(self.db)
        cols = []
        # get collection cursor objs
        for col_name in col_names:
            cols.append(mg.get_col(self.db, col_name))
        mg.update_tasks(cols, hostname)

        # remove the node(document) from WorkersInfo collection
        mg.delete_document(self.workersInfoCol, 'hostname', hostname)

        data = {'hostname': hostname}
        url = 'http://%s:%s/RESTfulSwarm/GM/request_leave' % (self.gm_address, SystemConstants.GM_PORT)
        print(requests.post(url=url, json=data).content)

    def dump_container(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/checkpoint_cons' % (self.gm_address, SystemConstants.GM_PORT)
        print(requests.post(url=url, json=data).content)

    def new_job_notify(self):
        job_queue = []
        timer = time.time()

        def pre_process_job(_msg):
            # read db, parse job resources request
            job_name = _msg[1]
            job_col = mg.get_col(self.db, job_name)
            col_data = mg.find_col(job_col)[0]
            # core request format: {$task_name: $core}}
            core_requests = {}
            # memory request format: {$task_name: $mem}
            mem_requests = {}
            target_nodes = []
            target_cpuset = []
            for task in col_data['job_info']['tasks'].items():
                core_requests.update({task[0]: task[1]['req_cores']})
                mem_requests.update({task[0]: task[1]['mem_limit']})
                target_nodes.append(task[1]['node'])
                target_cpuset.append(task[1]['cpuset_cpus'])
            return core_requests, mem_requests, target_nodes, target_cpuset

        def schedule_resource(jobs_details):
            schedule = self.scheduler.schedule_resources(jobs_details)

            schedule_decision = schedule[0]
            waiting_decision = schedule[1]

            if len(schedule_decision) > 0:
                # update Job collection
                self.scheduler.update_job_info(schedule_decision)

                # update WorkersInfo collection
                self.scheduler.update_workers_info(schedule_decision)

                # update worker resource collection
                self.scheduler.update_worker_resource_info(schedule_decision)

            return waiting_decision

        def execute():
            nonlocal timer
            nonlocal job_queue
            while True:
                job_queue_snap_shoot = job_queue[:]
                if len(job_queue_snap_shoot) == 0:
                    continue
                elif time.time() - timer >= self.wait:
                    jobs_details = []
                    temp_job_queue = []
                    for index, _msg in enumerate(job_queue_snap_shoot[:]):
                        jobs_details.append((_msg[1], pre_process_job(_msg)))

                    waiting_decision = schedule_resource(jobs_details)

                    # remove scheduled jobs
                    for index, job in enumerate(job_queue_snap_shoot[:]):
                        if job[1] not in waiting_decision:
                            temp_job_queue.append(job)
                            if job in job_queue:
                                job_queue.remove(job)
                            job_queue_snap_shoot.remove(job)

                    for _msg in temp_job_queue:
                        url = 'http://%s:%s/RESTfulSwarm/GM/request_new_job' % (self.gm_address,
                                                                                SystemConstants.GM_PORT)
                        job_name = _msg[1]
                        job_col = mg.get_col(self.db, job_name)
                        col_data = mg.find_col(job_col)[0]

                        del col_data['_id']
                        print(requests.post(url=url, json=col_data).content)

                    timer = time.time()

        execute_thr = threading.Thread(target=execute, args=())
        execute_thr.setDaemon(True)
        execute_thr.start()

        while True:
            msg = self.socket.recv_string()
            msg = msg.split()
            if msg[0] == 'SwitchScheduler':
                # make sure no job in job queue
                job_queue = []
                self.socket.send_string('Ack')
                new_scheduler = msg[1]
                if new_scheduler == 'first-fit':
                    self.scheduler = FirstFitScheduler(self.db)
                elif new_scheduler == 'best-fit':
                    self.scheduler = BestFitScheduler(self.db)
                elif new_scheduler == 'best-fit-decreasing':
                    self.scheduler = BestFitDecreasingScheduler(self.db)
                elif new_scheduler == 'first-fit-decreasing':
                    self.scheduler = FirstFitDecreasingScheduler(self.db)
                elif new_scheduler == 'no-scheduler':
                    self.scheduler = NodeScheduler(self.db)
            else:
                self.socket.send_string('Ack')
                job_queue.append(msg)


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--GM', type=str, help='Global manager node address.')
    # parser.add_argument('--db', type=str, help='MongoDB node address.')
    # parser.add_argument('-w', '--wait', type=int, default=3, help='Waiting time for Job Manager in seconds.')
    # parser.add_argument('-s', '--scheduling', type=str, choices=['first-fit', 'best-fit'], default='best-fit',
    #                     help='Scheduling algorithm option.')
    #
    # args = parser.parse_args()
    # gm_address = args.GM
    #
    # db_address = args.db
    #
    # scheduling_strategy = args.scheduling
    #
    # wait = args.wait

    os.chdir('/home/%s/RESTfulSwarmLM/JobManager' % utl.get_username())

    with open('JobManagerInit.json') as f:
        data = json.load(f)
    gm_address = data['gm_address']

    db_address = data['db_address']

    wait = data['wait_time']
    
    scheduling_strategy = data['scheduling_strategy']
    for strategy in scheduling_strategy:
        if scheduling_strategy[strategy] == 1:
            scheduling_strategy = strategy
            break

    db_client = mg.get_client(address=db_address, port=SystemConstants.MONGODB_PORT)
    db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)

    # choose scheduler
    # default scheduling strategy is best-fit
    if scheduling_strategy == 'first-fit':
        scheduler = FirstFitScheduler(db)
    elif scheduling_strategy == 'first-fit-decreasing':
        scheduler = FirstFitDecreasingScheduler(db)
    elif scheduling_strategy == 'best-fir-decreasing':
        scheduler = BestFitDecreasingScheduler(db)
    elif scheduling_strategy == 'no-scheduler':
        scheduler = NodeScheduler(db)
    else:
        scheduler = BestFitScheduler(db)

    job_manager = JobManager(gm_address=gm_address, db=db, scheduler=scheduler, wait=wait)

    fe_notify_thr = threading.Thread(target=job_manager.new_job_notify(), args=())
    fe_notify_thr.setDaemon(True)
    fe_notify_thr.start()

    job_manager.init_gm()

    os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())

    # while True:
    #     pass
    #     print('--------------RESTfulSwarm Menu--------------')
    #     print('1. Init Swarm')
    #     print('2. Create task(one container)')
    #     print('3. Check point a group containers')
    #     print('4. Migrate a container')
    #     print('5. Migrate a group of containers')
    #     print('6. Update Container')
    #     print('7. Leave Swarm')
    #     print('8. Describe Workers')
    #     print('9. Describe Manager')
    #     print('10. Exit')
    #     try:
    #         get_input = int(input('Please enter your choice: '))
    #         if get_input == 1:
    #             job_manager.init_gm()
    #         elif get_input == 2:
    #             json_path = input('Task Json file path: ')
    #             with open(json_path, 'r') as f:
    #                 data = json.load(f)
    #             job_manager.new_task(data)
    #         elif get_input == 3:
    #             json_path = input('Checkpoint Json file path: ')
    #             with open(json_path, 'r') as f:
    #                 data = json.load(f)
    #             job_manager.dump_container(data)
    #         elif get_input == 4:
    #             json_path = input('Migration Json file path: ')
    #             with open(json_path, 'r') as f:
    #                 data = json.load(f)
    #             job_manager.do_migrate(data)
    #         elif get_input == 5:
    #             migrate_json = input('Group migration Json file: ')
    #             with open(migrate_json, 'r') as f:
    #                 data = json.load(f)
    #             job_manager.do_group_migration(data)
    #         elif get_input == 6:
    #             json_path = input('New resource configuration Json file:')
    #             with open(json_path, 'r') as f:
    #                 data = json.load(f)
    #             job_manager.update_container(data)
    #         elif get_input == 7:
    #             hostname = input('Node hostname: ')
    #             job_manager.leave_swarm(hostname)
    #         elif get_input == 8:
    #             hostname = input('Node hostname: ')
    #             url = 'http://%s:%s/RESTfulSwarm/GM/%s/describe_worker' % (gm_address, SystemConstants.GM_PORT,
    #                                                                        hostname)
    #             print(requests.get(url=url).content.decode('utf-8'))
    #         elif get_input == 9:
    #             hostname = input('Node hostname: ')
    #             url = 'http://%s:%s/RESTfulSwarm/GM/%s/describe_manager' % (gm_address, SystemConstants.GM_PORT,
    #                                                                         hostname)
    #             print(requests.get(url=url).content.decode('utf-8'))
    #         elif get_input == 10:
    #             print('Thanks for using RESTfulSwarm, bye.')
    #             break
    #     except ValueError as er:
    #         print(er)


if __name__ == '__main__':
    main()