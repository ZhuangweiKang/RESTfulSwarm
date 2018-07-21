#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.Scheduler import Scheduler


class RandomScheduler(Scheduler):
    def __init__(self, db, workers_col_name, worker_resource_col_name):
        super(RandomScheduler, self).__init__(db, workers_col_name, worker_resource_col_name)

    def scheduling_algorithm(self, req_cores, free_cores):
        '''
        Best fit algorithm for scheduling resources
        :param req_cores: a list of requested resources (cpu cores)
        :param free_cores: a list of free resources
        :return: A list of tuples, best fit result [($request_index, $resource_index)]
        '''
        return None

