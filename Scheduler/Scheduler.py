#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import MongoDBHelper as mg
import utl


class Scheduler(object):
    __metaclass__ = ABCMeta

    def __init__(self, db, workers_col_name):
        self.db = db
        self.workers_col = mg.get_col(self.db, workers_col_name)

    @abstractmethod
    def schedule_resources(self, core_request, mem_request):
        return

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
        flag = 0
        job_index = 0
        result = []
        if schedule is not None:
            for index, item in enumerate(schedule):
                # get the first n cores from all free cores because the amount
                # of free cores may be more than requested cores
                temp1 = []
                if index == len(core_request[job_index][1].items()):
                    job_index += 1

                # Bug!!!!!!!!!!!!!
                for j in range(list(core_request[job_index][1].values())[item[0]]):
                    temp1.append(list(available_workers.values())[item[1]][flag])
                    flag += 1

                temp = (core_request[job_index][0],
                        list(core_request[job_index][1].keys())[item[0]],
                        list(available_workers.keys())[item[1]],
                        temp1)
                result.append(temp)

                # update free memory
                worker_info = list(self.workers_col.find({'hostname': list(available_workers.keys())[item[1]]}))[0]

                new_free_mem = utl.memory_size_translator(worker_info['MemFree'])
                request_mem = utl.memory_size_translator(mem_request_arr[index])

                new_free_mem -= request_mem
                new_free_mem = str(new_free_mem) + 'm'
                mg.update_doc(self.workers_col, 'hostname', list(available_workers.keys())[item[1]], 'MemFree',
                              new_free_mem)
            return result
        else:
            return None

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