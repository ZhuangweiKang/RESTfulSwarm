#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import MongoDBHelper as mg


class Scheduler:
    def __init__(self, db, workers_col_name):
        self.db = db
        self.workers_col = mg.get_col(self.db, workers_col_name)

    def check_resources(self, core_request):
        '''
        Check if we have enough capacity to deploy a job
        :param workers_col: workers information collection
        :param core_request: amount of cores requested by the job
        :param mem_request: amount of memory requested by the job
        :return: a list of nodes or None ($task_name, $worker_name, [$core])
        '''
        # core_request format: {$task_name: $core}

        available_workers = {}
        for worker in self.workers_col.find({}):
            available_workers.update({worker['hostname']: []})
            for cpu in worker['CPUs']:
                if worker['CPUs'][cpu] is False:
                    available_workers[worker['hostname']].append(cpu)
        requests = core_request.values()
        available = []
        for item in available_workers.values():
            available.append(len(item))
        bf_result = self.best_fit(requests, available)
        result = []
        if bf_result is not None:
            for item in bf_result:
                temp = (list(core_request.keys())[item[0]],
                        list(available_workers.keys())[item[1]],
                        list(available_workers.values())[item[1]])
                result.append(temp)
            return result
        else:
            return None

    def update_job_info(self, job_name, resource_check_result):
        for item in resource_check_result:
            job_col = mg.get_col(self.db, job_name)
            job_filter = 'job_info.tasks.%s.name' % item[0]
            target = 'job_info.tasks.%s.node' % item[0]
            job_col.update({job_filter: item[0]}, {target: item[1]}, upsert=True)
            target = 'job_info.tasks.%s.cpuset_cpus' % item[0]
            job_col.update({job_filter: item[0]}, {target: ','.join(item[2])}, upsert=True)

    def update_workers_info(self, resource_check_result):
        for item in resource_check_result:
            for core in item[2]:
                self.workers_col.update({'hostname': item[1]}, {str(core): True})

    def best_fit(self, request, available):
        '''
        Best fit algorithm for scheduling resources
        :param request: a list of requested resources (cpu cores)
        :param available: a list of free resources
        :return: A list of tuples, best fit result ($request_index: $resource_index) if scheduling successful, or None if failed
        '''
        result = []
        for j, req in enumerate(request):
            temp = []
            for index, res in enumerate(available[:]):
                if res >= req:
                    temp.append((index, res-req))
            if len(temp) > 0:
                min_index = temp[0][0]
                min_val = temp[0][1]
                for i in range(len(temp)):
                    if temp[i][1] < min_val:
                        min_index = temp[i][0]
                        min_val = temp[i][1]
                available[min_index] -= req
                result.append((j, min_index))
        if len(result) == len(request):
            return result
        else:
            return None

    def find_container(self, container_name):
        '''
        Find the worker node the container is locating
        :param container_name:
        :return: return node hostname
        '''
        collections = self.db.list_all_collection_names()
        temp = []
        for collection in collections:
            filter_key = 'job_info.tasks.%s.name' % container_name
            jobs_col = mg.get_col(self.db, collection)
            temp = list(jobs_col.find({filter_key: container_name}))
            if len(temp) != 0:
                break
        if len(temp) == 0:
            return None
        else:
            temp = temp[0]
            return temp['job_info']['tasks'][container_name]['node']
