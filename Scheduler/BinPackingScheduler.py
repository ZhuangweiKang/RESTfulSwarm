#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import mongodb_api as mg
import utl
from Scheduler.Scheduler import Scheduler


class BinPackingScheduler(Scheduler):
    __metaclass__ = ABCMeta

    def __init__(self, db):
        super(BinPackingScheduler, self).__init__(db)

    @abstractmethod
    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass

    def schedule_resources(self, jobs_details):
        # '''
        # Check if we have enough capacity to deploy a job
        # :param jobs_details: [($job_name, [{$task_name: $cpu_count}, {$task_name: $mem_limit}])]
        # :return: [$($job_name, $task_name, $worker_name, [$core])] + [$waiting_job]
        # '''

        # core_requests = [(job_name, {task1: cpu_count})]
        core_requests = [(job[0], job[1][0]) for job in jobs_details]
        # mem_request = [(job_name, {task1: mem_limit})]
        mem_requests = [(job[0], job[1][1]) for job in jobs_details]

        # get all free cores from every worker node
        available_workers = self.collect_free_cores()

        # available free cores of each worker node
        free_cores = list(map(lambda _item: len(_item), list(available_workers.values())))

        # apply core scheduling algorithm on data
        bf_result = self.cores_scheduling_algorithm(jobs_details=jobs_details, free_cores=free_cores)

        # requested mem_limit for each task
        mem_request_arr = []
        for _item in mem_requests:
            mem_request_arr.extend(list(_item[1].values()))

        # process schedule result
        return self.process_cores_scheduling_result(bf_result, core_requests, mem_request_arr, available_workers)

    def process_cores_scheduling_result(self, schedule, core_request, mem_request_arr, available_workers):
        job_index = 0
        result = []
        waiting_plan = []
        global_task_index = 0
        next_job = False
        task_index = 0
        temp_result = []

        # print(schedule)
        for index, item in enumerate(schedule):
            if next_job is False:
                global_task_index += len(core_request[job_index][1].items())
                next_job = True

            # item=(task_index, assigned_core_id/-1)
            if item[1] != -1:
                # get the first n cores from all free cores because the amount
                # of free cores may be more than requested cores
                cores = []
                for j in range(list(core_request[job_index][1].values())[task_index]):
                    cores.append(list(available_workers.values())[item[1]][0])
                    # remove used cores
                    key = list(available_workers.keys())[item[1]]
                    available_workers[key].pop(0)

                result_item = (core_request[job_index][0], list(core_request[job_index][1].keys())[task_index],
                               list(available_workers.keys())[item[1]], cores)

                temp_result.append(result_item)

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
                if len(temp_result) == len(core_request[job_index][1]):
                    result.extend(temp_result)
                else:
                    waiting_plan.append(core_request[job_index][0])
                job_index += 1
                next_job = False
                task_index = 0
                temp_result = []
            else:
                task_index += 1

        waiting_plan = list(set(waiting_plan))

        return result, waiting_plan