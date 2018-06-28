#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.Scheduler import Scheduler


class BestFitScheduler(Scheduler):
    def __init__(self, db, workers_col_name):
        super(Scheduler, self).__init__(db, workers_col_name)

    def schedule_resources(self, core_request, mem_request):
        '''
        Check if we have enough capacity to deploy a job
        :param core_request: [($job_name, ${$task: $core_count})]
        :param mem_request: [($job_name, ${$task: $mem})]
        :return: a list of tuples or None [$($job_name, $task_name, $worker_name, [$core])]
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
        bf_result = self.best_fit(req_cores, free_cores)

        # requested mem_limit for each task
        mem_request_arr = []
        for item in mem_request:
            mem_request_arr.extend(list(item[1].values()))

        # process schedule result
        return self.process_schedule_result(bf_result, core_request, mem_request_arr, available_workers)

    def best_fit(self, req_cores, free_cores):
        '''
        Best fit algorithm for scheduling resources
        :param req_cores: a list of requested resources (cpu cores)
        :param free_cores: a list of free resources
        :return: A list of tuples, best fit result [($request_index, $resource_index)] if scheduling successful, or None if failed
        '''
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