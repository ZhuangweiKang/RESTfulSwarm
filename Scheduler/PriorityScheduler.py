#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


from Scheduler.Scheduler import Scheduler


class PriorityScheduler(Scheduler):
    def __init__(self, db, workers_col_name, worker_resource_col_name):
        super(PriorityScheduler, self).__init__(db, workers_col_name, worker_resource_col_name)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass