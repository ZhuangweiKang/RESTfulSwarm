#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.BinPackingScheduler import BinPackingScheduler


class FirstFitScheduler(BinPackingScheduler):
    def __init__(self, db):
        super(FirstFitScheduler, self).__init__(db)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        core_requests = [(job[0], job[1][0]) for job in jobs_details]
        req_cores = []
        for item in core_requests:
            req_cores.extend(list(item[1].values()))
        return self.first_fit(requested_resources=req_cores, free_resources=free_cores)

    @staticmethod
    def first_fit(requested_resources, free_resources):
        # '''
        # First fit algorithm for scheduling resources
        # :param requested_resources: a list of requested resources
        # :param free_resources: a list of free resources
        # :return: A list of tuples, best fit result [($request_index, $resource_index)]
        # '''
        result = []
        for i, req in enumerate(requested_resources[:]):
            fit = False
            for j, free in enumerate(free_resources[:]):
                if req <= free:
                    free_resources[j] -= req
                    fit = True
                    result.append((i, j))
                    break
            # free resources are not enough
            if fit is False:
                result.append((i, -1))
        return result