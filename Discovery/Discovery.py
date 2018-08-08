#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import os
import time
import json
import argparse

import mongodb_api as mg
import SystemConstants
import zmq_api as zmq
import utl


class Discovery(object):
    def __init__(self, __db_address):
        db_client = mg.get_client(address=__db_address, port=SystemConstants.MONGODB_PORT)
        self.__db = mg.get_db(db_client, SystemConstants.MONGODB_NAME)
        self.__workers_info = mg.get_col(self.__db, SystemConstants.WorkersInfo)
        self.__workers_resource_info = mg.get_col(self.__db, SystemConstants.WorkersResourceInfo)
        self.__socket = zmq.cs_bind(SystemConstants.DISCOVERY_PORT)
        self.logger = utl.get_logger('DiscoveryLogger', 'DiscoveryLog.log')

    def discovery(self):
        deployed_tasks = []
        while True:
            try:
                msg_pack = self.__socket.recv_string()
                self.logger.info('Recv msg: %s' % msg_pack)
                self.__socket.send_string('Ack')
                msg_pack = msg_pack.split(',')
                for msg in msg_pack:
                    worker_host = msg.split()[0]
                    msg = msg.split()[1]
                    task_name = msg
                    if task_name in deployed_tasks:
                        continue
                    else:
                        deployed_tasks.append(task_name)
                    job_name = msg.split('_')[0]
                    job_col = mg.get_col(self.__db, job_name)

                    # update job collection -- task status
                    filter_key = 'job_info.tasks.%s.container_name' % task_name
                    target_key = 'job_info.tasks.%s.status' % task_name
                    mg.update_doc(job_col, filter_key, task_name, target_key, 'Down')

                    # update job status if necessary
                    job_details = mg.find_col(job_col)[0]
                    flag = True
                    for job in job_details['job_info']['tasks']:
                        if job_details['job_info']['tasks'][job]['status'] != 'Down':
                            flag = False
                    if flag:
                        mg.update_doc(job_col, 'job_name', job_name, 'status', 'Down')
                        mg.update_doc(job_col, 'job_name', job_name, 'end_time', time.time())

                    self.logger.info('Updating Job collection %s.' % job_name)

                    # get the resource utilization of the 'Down' container
                    job_info = mg.find_col(job_col)[0]
                    cores = job_info['job_info']['tasks'][task_name]['cpuset_cpus']
                    cores = cores.split(',')
                    memory = job_info['job_info']['tasks'][task_name]['mem_limit']
                    self.logger.info('Collecting resources from down containers.')

                    # update WorkersInfo collection
                    # update cores info
                    for core in cores:
                        target_key = 'CPUs.%s' % core
                        mg.update_doc(self.__workers_info, 'hostname', worker_host, target_key, False)
                        self.logger.info('Release core %s status in worker %s' % (target_key, worker_host))

                    # update memory info
                    worker_info = mg.filter_col(self.__workers_info, 'hostname', worker_host)
                    free_memory = worker_info['MemFree']
                    memory = float(memory.split('m')[0])
                    free_memory = float(free_memory.split('m')[0])
                    updated_memory = memory + free_memory
                    updated_memory = str(updated_memory) + 'm'
                    mg.update_doc(self.__workers_info, 'hostname', worker_host, 'MemFree', updated_memory)
                    self.logger.info('Updating memory resources in WorkersInfo collection.')

                    # update worker resource collection
                    mg.update_workers_resource_col(self.__workers_info, worker_host, self.__workers_resource_info)
                    self.logger.info('Updated WorkersResourceInfo collection, because some cores are released.')

                    # update job collection -- cpuset_cpus
                    target_key = 'job_info.tasks.%s.cpuset_cpus' % task_name
                    mg.update_doc(job_col, filter_key, task_name, target_key, '')
                    self.logger.info('Updated Job collection. Released used cores.')

                    # update job collection -- mem_limit
                    target_key = 'job_info.tasks.%s.mem_limit' % task_name
                    mg.update_doc(job_col, filter_key, task_name, target_key, '')
                    self.logger.info('Updated Job collection. Released used memory.')
            except Exception as ex:
                self.logger.error(ex)

    @staticmethod
    def main():
        os.chdir('/home/%s/RESTfulSwarmLM/Discovery' % utl.get_username())

        with open('DiscoveryInit.json') as f:
            data = json.load(f)

        db_address = data['db_address']

        discovery = Discovery(db_address)
        discovery.logger.info('Initialized Discovery block.')
        discovery.discovery()

        os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('--db', type=str, help='Mongodb server address.')
    # args = parser.parse_args()
    # db_address = args.db

    Discovery.main()