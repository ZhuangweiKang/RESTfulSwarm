#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import MongoDBHelper as mg
import time
import utl


class Scheduler(object):
    __metaclass__ = ABCMeta

    def __init__(self, db, workers_col_name, worker_resource_col_name):
        self.db = db
        self.workers_col = mg.get_col(self.db, workers_col_name)
        self.workers_resource_col = mg.get_col(self.db, worker_resource_col_name)

    @abstractmethod
    def scheduling_algorithm(self, req_cores, free_cores):
        pass

    def schedule_resources(self, core_request, mem_request):
        '''
        Check if we have enough capacity to deploy a job
        :param core_request: [($job_name, ${$task: $core_count})]
        :param mem_request: [($job_name, ${$task: $mem})]
        :return: [$($job_name, $task_name, $worker_name, [$core])] + [$waiting_job]
        '''
        # get all free cores from every worker node
        available_workers = self.collect_free_cores()

        # requested cores number for each task, etc. [2, 3, 1]
        req_cores = []
        for item in core_request:
            req_cores.extend(list(item[1].values()))

        # available free cores of each worker node
        free_cores = []
        for item in list(available_workers.values()):
            free_cores.append(len(item))

        # apply schedule algorithm on data
        bf_result = self.scheduling_algorithm(req_cores, free_cores)

        # requested mem_limit for each task
        mem_request_arr = []
        for item in mem_request:
            mem_request_arr.extend(list(item[1].values()))

        # process schedule result
        return self.process_schedule_result(bf_result, core_request, mem_request_arr, available_workers)

    def collect_free_cores(self):
        # get all free cores from every worker node
        available_workers = {}
        for worker in list(self.workers_col.find({})):
            available_workers.update({worker['hostname']: []})
            for cpu in worker['CPUs'].keys():
                if worker['CPUs'][cpu] is False:
                    available_workers[worker['hostname']].append(cpu)
        return available_workers

    def process_schedule_result(self, schedule, core_request, mem_request_arr, available_workers):
        print('Schedule:')
        print(schedule)

        print('Core Requests:')
        print(core_request)

        print('Memory request array:')
        print(mem_request_arr)

        print('Available Workers:')
        print(available_workers)

        job_index = 0
        result = []
        waiting_plan = []
        global_task_index = 0
        next_job = False
        task_index = 0

        for index, item in enumerate(schedule):
            if next_job is False:
                global_task_index += len(core_request[job_index][1].items())
                next_job = True

            if core_request[job_index][0] not in waiting_plan and item[1] != -1:
                # get the first n cores from all free cores because the amount
                # of free cores may be more than requested cores
                cores = []
                for j in range(list(core_request[job_index][1].values())[task_index]):
                    cores.append(list(available_workers.values())[item[1]][0])
                    # remove used cores
                    key = list(available_workers.keys())[item[1]]
                    available_workers[key].pop(0)

                result_item = (core_request[job_index][0],
                        list(core_request[job_index][1].keys())[task_index],
                        list(available_workers.keys())[item[1]],
                        cores)
                result.append(result_item)

                # update free memory
                worker_info = list(self.workers_col.find({'hostname': list(available_workers.keys())[item[1]]}))[0]

                new_free_mem = utl.memory_size_translator(worker_info['MemFree'])
                request_mem = utl.memory_size_translator(mem_request_arr[index])

                new_free_mem -= request_mem
                new_free_mem = str(new_free_mem) + 'm'
                mg.update_doc(self.workers_col, 'hostname', list(available_workers.keys())[item[1]], 'MemFree',
                              new_free_mem)
            else:
                # if resources are not enough, add the job into waiting list
                waiting_plan.append(core_request[job_index][0])

            # update job index
            if index == global_task_index - 1:
                job_index += 1
                next_job = False
                task_index = 0
            else:
                task_index += 1

        waiting_plan = list(set(waiting_plan))

        return result, waiting_plan

    def update_job_info(self, schedule):
        for item in schedule:
            job_col = mg.get_col(self.db, item[0])
            job_filter = 'job_info.tasks.%s.container_name' % item[1]

            # add node filed
            target = 'job_info.tasks.%s.node' % item[1]
            mg.update_doc(job_col, job_filter, item[1], target, item[2])

            # add cpuset_cpus field
            target = 'job_info.tasks.%s.cpuset_cpus' % item[1]
            mg.update_doc(job_col, job_filter, item[1], target, ','.join(item[3]))

    def update_workers_info(self, schedule):
        for item in schedule:
            for core in item[3]:
                target = 'CPUs.%s' % str(core)
                mg.update_doc(self.workers_col, 'hostname', item[2], target, True)

    # update worker resource collection
    def update_worker_resource_info(self, schedule):
        for item in schedule:
            target_worker_info = mg.filter_col(self.workers_col, 'hostname', item[2])
            used_core_num = 0
            free_core_num = 0
            for core in target_worker_info['CPUs'].keys():
                if target_worker_info['CPUs'][core]:
                    used_core_num += 1
                else:
                    free_core_num += 1
            used_core_ratio = used_core_num / (used_core_num + free_core_num)
            free_core_ratio = free_core_num / (used_core_num + free_core_num)
            time_stamp = int(time.time())
            filter_result = mg.filter_col(self.workers_resource_col, 'time', time_stamp)
            if filter_result is None:
                resource_info = {
                    'time': time_stamp,
                    'details': {
                        item[2]: [used_core_ratio, free_core_ratio, used_core_num + free_core_num]
                    }
                }
                mg.insert_doc(self.workers_resource_col, resource_info)
            else:
                filter_result['details'].update({item[2]: [used_core_ratio, free_core_ratio, used_core_num + free_core_num]})
                mg.update_doc(self.workers_resource_col, 'time', time_stamp, 'details', filter_result['details'])

    def find_container(self, container_name):
        '''
        Find the worker node the container is locating
        :param container_name:
        :return: return node hostname
        '''
        collections = self.db.collection_names(include_system_collections=False)
        temp = []
        for collection in collections:
            filter_key = 'job_info.tasks.%s.container_name' % container_name
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