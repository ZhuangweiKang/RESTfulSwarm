#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import os, sys
import traceback
import time
import json
import argparse

import mongodb_api as mg
import SystemConstants
from Messenger import Messenger
import utl


class Discovery(object):
    def __init__(self, db_info):
        _db_client = mg.get_client(usr=db_info['user'], pwd=db_info['pwd'], db_name=db_info['db_name'],
                                   address=db_info['address'], port=SystemConstants.MONGODB_PORT)
        self.__db = mg.get_db(_db_client, SystemConstants.MONGODB_NAME)
        self.__workers_info = mg.get_col(self.__db, SystemConstants.WorkersInfo)
        self.__workers_resource_info = mg.get_col(self.__db, SystemConstants.WorkersResourceInfo)
        self.__messenger = Messenger(messenger_type='C/S', port=SystemConstants.DISCOVERY_PORT)
        self.__logger = utl.get_logger('DiscoveryLogger', 'DiscoveryLog.log')

    def discovery(self):
        deployed_tasks = []

        def update_db(msg):
            worker_host = msg.split()[0]
            msg = msg.split()[1]
            task_name = msg
            if task_name not in deployed_tasks:
                deployed_tasks.append(task_name)
                job_name = msg.split('_')[0]
                job_col = mg.get_col(self.__db, job_name)

                # update job collection -- task status
                filter_key = 'job_info.tasks.%s.container_name' % task_name
                target_key = 'job_info.tasks.%s.status' % task_name
                mg.update_doc(job_col, filter_key, task_name, target_key, 'Down')

                job_details = mg.find_col(job_col)[0]

                # update job status if all tasks are down
                flag = True
                for task in job_details['job_info']['tasks']:
                    if job_details['job_info']['tasks'][task]['status'] != 'Down':
                        flag = False
                if flag:
                    mg.update_doc(job_col, 'job_name', job_name, 'status', 'Down')
                    mg.update_doc(job_col, 'job_name', job_name, 'end_time', time.time())

                self.__logger.info('Updating Job collection %s.' % job_name)

                # get the resource utilization of the 'Down' container
                cores = job_details['job_info']['tasks'][task_name]['cpuset_cpus']
                cores = cores.split(',')
                memory = job_details['job_info']['tasks'][task_name]['mem_limit']
                self.__logger.info('Collecting resources from down containers.')

                # update WorkersInfo collection
                # update cores info
                def update_cores(core):
                    _target_key = 'CPUs.%s' % core
                    mg.update_doc(self.__workers_info, 'hostname', worker_host, _target_key, False)
                    self.__logger.info('Release core %s status in worker %s' % (_target_key, worker_host))
                [update_cores(core) for core in cores]

                # update memory info
                worker_info = mg.filter_col(self.__workers_info, 'hostname', worker_host)
                free_memory = worker_info['MemFree']
                memory = float(memory.split('m')[0])
                free_memory = float(free_memory.split('m')[0])
                updated_memory = memory + free_memory
                updated_memory = str(updated_memory) + 'm'
                mg.update_doc(self.__workers_info, 'hostname', worker_host, 'MemFree', updated_memory)
                self.__logger.info('Updating memory resources in WorkersInfo collection.')

                # update worker resource collection
                mg.update_workers_resource_col(self.__workers_info, worker_host, self.__workers_resource_info)
                self.__logger.info('Updated WorkersResourceInfo collection, because some cores are released.')

                # update job collection -- cpuset_cpus
                target_key = 'job_info.tasks.%s.cpuset_cpus' % task_name
                mg.update_doc(job_col, filter_key, task_name, target_key, '')
                self.__logger.info('Updated Job collection. Released used cores.')

                # update job collection -- mem_limit
                target_key = 'job_info.tasks.%s.mem_limit' % task_name
                mg.update_doc(job_col, filter_key, task_name, target_key, '')
                self.__logger.info('Updated Job collection. Released used memory.')

        while True:
            try:
                _msg = self.__messenger.receive(feedback='Ack')
                self.__logger.info('Recv msg: %s' % _msg)
                _msg = _msg.split(',')
                [update_db(msg) for msg in _msg]
            except Exception:
                traceback.print_exc(file=sys.stdout)

    @staticmethod
    def main():
        os.chdir('/home/%s/RESTfulSwarm/Discovery' % utl.get_username())

        with open('../DBInfo.json') as f:
            db_info = json.load(f)

        discovery = Discovery(db_info)
        discovery.__logger.info('Initialized Discovery block.')
        discovery.discovery()

        os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--db', type=str, help='Mongodb server address.')
    # args = parser.parse_args()
    # db_address = args.db

    Discovery.main()