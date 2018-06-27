#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import MongoDBHelper as mg
import utl


class Scheduler:
    def __init__(self, db, workers_col_name):
        self.db = db
        self.workers_col = mg.get_col(self.db, workers_col_name)

    def schedule_resources(self, core_request, mem_request):
        '''
        Check if we have enough capacity to deploy a job
        :param core_request: {$task_name: $core}
        :param mem_request: {$task_name: $mem}
        :return: a list of tuples or None [$($task_name, $worker_name, [$core])]
        '''
        # get all free cores from every worker node
        available_workers = {}
        for worker in list(self.workers_col.find({})):
            available_workers.update({worker['hostname']: []})
            for cpu in worker['CPUs'].keys():
                if worker['CPUs'][cpu] is False:
                    available_workers[worker['hostname']].append(cpu)

        # request cores number for each task, etc. [2, 3, 1]
        req_cores = list(core_request.values())

        # the amount of free cores of each worker node
        free_cores = []
        for item in available_workers.values():
            free_cores.append(len(item))

        bf_result = self.best_fit(req_cores, free_cores)

        result = []
        mem_request_arr = list(mem_request.values())

        flag = 0
        if bf_result is not None:
            for index, item in enumerate(bf_result):
                # get any n cores from all free cores because the amount of free cores may be more than requested cores
                temp1 = []
                for j in range(list(core_request.values())[item[0]]):
                    temp1.append(list(available_workers.values())[item[1]][flag])
                    flag += 1

                temp = (list(core_request.keys())[item[0]],
                        list(available_workers.keys())[item[1]],
                        temp1)
                result.append(temp)

                # update free memory
                worker_info = list(self.workers_col.find({'hostname': list(available_workers.keys())[item[1]]}))[0]
                new_free_mem = utl.memory_size_translator(worker_info['MemFree'])
                request_mem = utl.memory_size_translator(mem_request_arr[index])
                new_free_mem -= request_mem
                new_free_mem = str(new_free_mem) + 'm'
                mg.update_doc(self.workers_col, 'hostname', list(available_workers.keys())[item[1]], 'MemFree', new_free_mem)

            return result
        else:
            return None

    def update_job_info(self, job_name, schedule):
        for item in schedule:
            job_col = mg.get_col(self.db, job_name)
            job_filter = 'job_info.tasks.%s.name' % item[0]

            # add node filed
            target = 'job_info.tasks.%s.node' % item[0]
            mg.update_doc(job_col, job_filter, item[0], target, item[1])

            # add cpuset_cpus field
            target = 'job_info.tasks.%s.cpuset_cpus' % item[0]
            mg.update_doc(job_col, job_filter, item[0], target, ','.join(item[2]))

    def update_workers_info(self, schedule):
        for item in schedule:
            for core in item[2]:
                target = 'CPUs.%s' % str(core)
                mg.update_doc(self.workers_col, 'hostname', item[1], target, True)

    def best_fit(self, req_cores, free_cores):
        '''
        Best fit algorithm for scheduling resources
        :param req_cores: a list of requested resources (cpu cores)
        :param free_cores: a list of free resources
        :return: A list of tuples, best fit result [($request_index, $resource_index)] if scheduling successful, or None if failed
        '''
        print(req_cores, free_cores)
        result = []
        for j, req in enumerate(req_cores):
            temp = []
            for index, res in enumerate(free_cores[:]):
                if res >= req:
                    temp.append((index, res-req))
            if len(temp) > 0:
                min_index = temp[0][0]
                min_val = temp[0][1]
                for i in range(len(temp)):
                    if temp[i][1] < min_val:
                        min_index = temp[i][0]
                        min_val = temp[i][1]
                free_cores[min_index] -= req
                result.append((j, min_index))
            else:
                return None
        return result

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

    def get_node_info(self, node_name):
        '''
        Get node information by node_name
        :param node_name:
        :return: A python dict if the node is available, or None if not
        '''
        try:
            return list(self.workers_col.find({'hostname': node_name}))[0]
        except Exception:
            return None