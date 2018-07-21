#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.Scheduler import Scheduler


class BestFitScheduler(Scheduler):
    def __init__(self, db, workers_col_name, worker_resource_col_name):
        super(BestFitScheduler, self).__init__(db, workers_col_name, worker_resource_col_name)

    def scheduling_algorithm(self, req_cores, free_cores):
        '''
        Best fit algorithm for scheduling resources
        :param req_cores: a list of requested resources (cpu cores)
        :param free_cores: a list of free resources
        :return: A list of tuples, best fit result [($request_index, $resource_index)]
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
                # cores are not enough
                result.append((j, -1))
        return result