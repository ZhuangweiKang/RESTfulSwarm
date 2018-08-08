#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


from Scheduler.Scheduler import Scheduler


class PriorityScheduler(Scheduler):
    def __init__(self, db):
        super(PriorityScheduler, self).__init__(db)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass