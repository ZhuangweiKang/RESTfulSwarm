#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from Scheduler.BinPackingScheduler import BinPackingScheduler


class BestFitScheduler(BinPackingScheduler):
    def __init__(self, db):
        super(BestFitScheduler, self).__init__(db)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        core_requests = [(job[0], job[1][0]) for job in jobs_details]
        req_cores = []
        for item in core_requests:
            req_cores.extend(list(item[1].values()))
        return self.best_fit(requested_resources=req_cores, free_resources=free_cores)

    @staticmethod
    def best_fit(requested_resources, free_resources):
        # '''
        # Best fit algorithm for scheduling resources
        # :param requested_resources: a list of requested resources
        # :param free_resources: a list of free resources
        # :return: A list of tuples, best fit result [($request_index, $resource_index)]
        # '''
        result = []
        for j, req in enumerate(requested_resources):
            temp = []
            for index, res in enumerate(free_resources[:]):
                if res >= req:
                    temp.append((index, res - req))
            if len(temp) > 0:
                min_index = temp[0][0]
                min_val = temp[0][1]
                for i in range(len(temp)):
                    if temp[i][1] < min_val:
                        min_index = temp[i][0]
                        min_val = temp[i][1]
                free_resources[min_index] -= req
                result.append((j, min_index))
            else:
                # free resources are not enough
                result.append((j, -1))
        return result