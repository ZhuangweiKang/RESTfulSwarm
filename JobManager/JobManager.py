#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
import requests
import utl
import time
import json
import threading

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from Scheduler.BestFitScheduler import BestFitScheduler
from Scheduler.FirstFitScheduler import FirstFitScheduler
from Scheduler.BestFitDecreasingScheduler import BestFitDecreasingScheduler
from Scheduler.FirstFitDecreasingScheduler import FirstFitDecreasingScheduler
from Scheduler.NodeScheduler import NodeScheduler
from Messenger import Messenger
import mongodb_api as mg
import SystemConstants


class JobManager(object):
    def __init__(self, gm_address, db, scheduler, wait):
        self.__gm_address = gm_address
        self.__db = db
        self.__scheduler = scheduler
        self.__workersInfoCol = self.__db['WorkersInfo']
        self.__workers_resource_col = self.__db['WorkersResourceInfo']
        self.__wait = wait

        # listening msg from FrontEnd
        self.__messenger = Messenger(messenger_type='C/S', port=SystemConstants.JM_PORT)

    # Initialize global manager(Swarm manager node)
    def init_gm(self):
        url = 'http://%s:%s/RESTfulSwarm/GM/init' % (self.__gm_address, SystemConstants.GM_PORT)
        requests.get(url=url)

    # Create a single task(container) using a Json file
    def new_task(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/request_new_task' % (self.__gm_address, SystemConstants.GM_PORT)
        requests.post(url=url, json=data)

    def container_migration(self, data):
        dest_node_name = (data['to'].split('@'))[0]
        dest_node_info = self.__scheduler.get_node_info(dest_node_name)
        if dest_node_info is None:
            er = 'Migration failed, because node %s is not in Swarm environment.'
            raise Exception(er)
        else:
            # update node name
            job_name = data['job']
            job_col = self.__db[job_name]
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
            src_node_name = self.__scheduler.find_container(data['container'])

            for _core in cores:
                mg.update_doc(self.__workersInfoCol, 'hostname', src_node_name, 'CPUs.%s' % _core, False)

            # mark cores in destination node as busy
            for _core in cores:
                mg.update_doc(self.__workersInfoCol, 'hostname', dest_node_name, 'CPUs.%s' % _core, True)

            # get free memory from both source node and destination node
            src_worker_info = list(self.__workersInfoCol.find({'hostname': src_node_name}))[0]
            src_free_mem = src_worker_info['MemFree']
            new_free_mem = dest_node_info['MemFree']

            # update memory field in both source node and destination node
            update_src_mem = str(utl.memory_size_translator(src_free_mem) + mem_limit) + 'm'
            update_dst_mem = str(utl.memory_size_translator(new_free_mem) - mem_limit) + 'm'

            mg.update_doc(self.__workersInfoCol, 'hostname', src_node_name, 'MemFree', update_src_mem)
            mg.update_doc(self.__workersInfoCol, 'hostname', dest_node_name, 'MemFree', update_dst_mem)

            # pre-process migration json file
            data.update({'from': data['from'].split('@')[1]})
            data.update({'to': data['to'].split('@')[1]})

            # update worker resource collection for both workers
            mg.update_workers_resource_col(workers_col=self.__workersInfoCol, hostname=src_node_name,
                                           workers_resource_col=self.__workers_resource_col)
            mg.update_workers_resource_col(workers_col=self.__workersInfoCol, hostname=dest_node_name,
                                           workers_resource_col=self.__workers_resource_col)
            return data

    # Migrate a container
    # !!!Note: Assuming destination node always has enough cores to hold the migrated container
    def do_migrate(self, data):
        try:
            data = self.container_migration(data)
            url = 'http://%s:%s/RESTfulSwarm/GM/request_migrate' % (self.__gm_address, SystemConstants.GM_PORT)
            requests.post(url=url, json=data)
            return True
        except Exception:
            return False

    def do_group_migration(self, data):
        try:
            for index, item in enumerate(data[:]):
                new_item = self.container_migration(item)
                data[index].update(new_item)
            url = 'http://%s:%s/RESTfulSwarm/GM/request_group_migration' % (self.__gm_address, SystemConstants.GM_PORT)
            requests.post(url=url, json=data)
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

        job_col = self.__db[job]
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
        for _core in current_cpu:
            mg.update_doc(self.__workersInfoCol, 'hostname', node, 'CPUs.%s' % _core, False)

        for _core in new_cpu:
            mg.update_doc(self.__workersInfoCol, 'hostname', node, 'CPUs.%s' % _core, True)

        # update memory, and we assuming memory unit is always m
        current_mem = utl.memory_size_translator(current_mem)
        worker_info = list(self.__workersInfoCol.find({'hostname': node}))[0]
        free_mem = utl.memory_size_translator(worker_info['MemFree'])
        current_mem = free_mem + current_mem
        new_mem = utl.memory_size_translator(new_mem)
        current_mem -= new_mem
        current_mem = str(current_mem) + 'm'
        mg.update_doc(self.__workersInfoCol, 'hostname', node, 'MemFree', current_mem)

        # update worker resource Info collection
        mg.update_workers_resource_col(workers_col=self.__workersInfoCol, hostname=node,
                                       workers_resource_col=self.__workers_resource_col)

        url = 'http://%s:%s/RESTfulSwarm/GM/request_update_container' % (self.__gm_address, SystemConstants.GM_PORT)
        requests.post(url=url, json=data)

    # Leave Swarm
    def leave_swarm(self, hostname):
        col_names = mg.get_all_cols(self.__db)
        cols = []
        # get collection cursor objs
        for col_name in col_names:
            cols.append(mg.get_col(self.__db, col_name))
        mg.update_tasks(cols, hostname)

        # remove the node(document) from WorkersInfo collection
        mg.delete_document(self.__workersInfoCol, 'hostname', hostname)

        data = {'hostname': hostname}
        url = 'http://%s:%s/RESTfulSwarm/GM/request_leave' % (self.__gm_address, SystemConstants.GM_PORT)
        requests.post(url=url, json=data)

    def dump_container(self, data):
        url = 'http://%s:%s/RESTfulSwarm/GM/checkpoint_cons' % (self.__gm_address, SystemConstants.GM_PORT)
        requests.post(url=url, json=data)

    def new_job_notify(self):
        job_queue = []
        timer = time.time()

        def pre_process_job(_msg):
            # read db, parse job resources request
            job_name = _msg
            job_col = mg.get_col(self.__db, job_name)
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
            schedule = self.__scheduler.schedule_resources(jobs_details)

            schedule_decision = schedule[0]
            waiting_decision = schedule[1]

            if len(schedule_decision) > 0:
                # update Job collection
                self.__scheduler.update_job_info(schedule_decision)

                # update WorkersInfo collection
                self.__scheduler.update_workers_info(schedule_decision)

                # update worker resource collection
                self.__scheduler.update_worker_resource_info(schedule_decision)

            return waiting_decision

        def execute():
            nonlocal timer
            nonlocal job_queue
            while True:
                job_queue_snap_shoot = job_queue[:]
                if len(job_queue_snap_shoot) == 0:
                    continue
                elif time.time() - timer >= self.__wait:
                    jobs_details = []
                    temp_job_queue = []

                    for _msg in job_queue_snap_shoot[:]:
                        jobs_details.append((_msg, pre_process_job(_msg)))

                    waiting_decision = schedule_resource(jobs_details)

                    # remove scheduled jobs
                    for job in job_queue_snap_shoot[:]:
                        if job not in waiting_decision:
                            temp_job_queue.append(job)
                            if job in job_queue:
                                job_queue.remove(job)
                            job_queue_snap_shoot.remove(job)

                    for _msg in temp_job_queue:
                        url = 'http://%s:%s/RESTfulSwarm/GM/request_new_job' % (self.__gm_address,
                                                                                SystemConstants.GM_PORT)
                        job_name = _msg
                        job_col = mg.get_col(self.__db, job_name)
                        col_data = mg.find_col(job_col)[0]

                        del col_data['_id']
                        requests.post(url=url, json=col_data)

                    timer = time.time()

        execute_thr = threading.Thread(target=execute, args=())
        execute_thr.setDaemon(True)
        execute_thr.start()

        while True:
            msg = self.__messenger.receive('Ack')
            msg = msg.split()
            if msg[0] == 'SwitchScheduler':
                # make sure no job in job queue
                job_queue = []
                new_scheduler = msg[1]
                self.__scheduler = {
                    'first-fit': FirstFitScheduler(self.__db),
                    'first-fit-decreasing': FirstFitDecreasingScheduler(self.__db),
                    'best-fit': BestFitScheduler(self.__db),
                    'best-fir-decreasing': BestFitDecreasingScheduler(self.__db),
                    'no-scheduler': NodeScheduler(self.__db)
                }.get(new_scheduler)
            elif msg[0] == 'newJob':
                job_queue.append(msg[1])

    @staticmethod
    def main():
        os.chdir('/home/%s/RESTfulSwarm/JobManager' % utl.get_username())

        with open('../ActorsInfo.json') as f:
            data = json.load(f)

        gm_address = data['GM']['address']

        data = data['JM']

        with open('../DBInfo.json') as f:
            db_info = json.load(f)

        wait = data['wait_time']

        scheduling_strategy = data['scheduling_strategy']
        for strategy in scheduling_strategy:
            if scheduling_strategy[strategy] == 1:
                scheduling_strategy = strategy
                break

        db_client = mg.get_client(usr=db_info['user'], pwd=db_info['pwd'], db_name=db_info['db_name'],
                                  address=db_info['address'], port=SystemConstants.MONGODB_PORT)
        db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)

        # choose scheduler
        # default scheduling strategy is best-fit
        scheduler = {
            'first-fit': FirstFitScheduler(db),
            'first-fit-decreasing': FirstFitDecreasingScheduler(db),
            'best-fit': BestFitScheduler(db),
            'best-fir-decreasing': BestFitDecreasingScheduler(db),
            'no-scheduler': NodeScheduler(db)
        }.get(scheduling_strategy, 'best-fit')

        job_manager = JobManager(gm_address=gm_address, db=db, scheduler=scheduler, wait=wait)

        fe_notify_thr = threading.Thread(target=job_manager.new_job_notify, args=())
        fe_notify_thr.setDaemon(True)
        fe_notify_thr.start()

        job_manager.init_gm()

        os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())

        while True:
            pass
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
        #             requests.get(url=url)
        #         elif get_input == 9:
        #             hostname = input('Node hostname: ')
        #             url = 'http://%s:%s/RESTfulSwarm/GM/%s/describe_manager' % (gm_address, SystemConstants.GM_PORT,
        #                                                                         hostname)
        #             requests.get(url=url)
        #         elif get_input == 10:
        #             print('Thanks for using RESTfulSwarm, bye.')
        #             break
        #     except ValueError as er:
        #         print(er)


if __name__ == '__main__':
    JobManager.main()