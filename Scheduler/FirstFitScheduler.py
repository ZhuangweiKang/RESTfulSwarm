#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.Scheduler import Scheduler


class FirstFitScheduler(Scheduler):
    def __init__(self, db, workers_col_name):
        super(FirstFitScheduler, self).__init__(db, workers_col_name)

    def scheduling_algorithm(self, req_cores, free_cores):
        '''
        First fit algorithm for scheduling resources
        :param req_cores: a list of requested resources (cpu cores)
        :param free_cores: a list of free resources
        :return: A list of tuples, best fit result [($request_index, $resource_index)]
        '''
        result = []
        for i, core in enumerate(req_cores[:]):
            fit = False
            for j, free_core in enumerate(free_cores[:]):
                if core <= free_core:
                    free_cores[j] -= core
                    fit = True
                    result.append((i, j))
            if fit is False:
                result.append((i, -1))
        return result