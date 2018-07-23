#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.FirstFitScheduler import FirstFitScheduler


class FirstFitDecreasingScheduler(FirstFitScheduler):
    def __init__(self, db, workers_col_name, worker_resource_col_name):
        super(FirstFitScheduler).__init__(db, workers_col_name, worker_resource_col_name)

    def cores_scheduling_algorithm(self, core_requests, free_cores):
        req_cores = []
        for item in core_requests:
            temp = list(item[1].values())
            temp.sort(reverse=True)
            req_cores.extend(temp)
        return self.first_fit(requested_resources=req_cores, free_resources=free_cores)