#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import MongoDBHelper as mg


class Scheduler:
    def __init__(self, db_address, db_port, db_name, workers_col_name):
        self.db_client = mg.get_client(address=db_address, port=db_port)
        self.db = mg.get_db(self.db_client, db_name=db_name)
        self.workers_col = mg.get_col(self.db, workers_col_name)

    def check_resources(self, core_request, mem_request):
        '''
        Check if we have enough capacity to deploy a job
        :param workers_col: workers information collection
        :param core_request: amount of cores requested by the job
        :param mem_request: amount of memory requested by the job
        :return: a list of nodes or None
        '''
        available_workers = {}
        for worker in self.workers_col.find({}):
            available_workers.update({worker['hostname']: []})
            for cpu in worker['CPUs']:
                if worker['CPUs'][cpu] is False:
                    available_workers[worker['hostname']].append(cpu)

    def find_container(self, container_name):
        '''
        Find the worker node the container is locating
        :param container_name:
        :return: return node hostname
        '''
        pass