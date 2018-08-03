#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import MongoDBHelper as mg
from Scheduler.Scheduler import Scheduler


class NoScheduler(Scheduler):
    def __init__(self, db, workers_col_name, worker_resource_col_name):
        super(NoScheduler, self).__init__(db, workers_col_name, worker_resource_col_name)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass

    def schedule_resources(self, jobs_details):
        '''
        Check if we have enough capacity to deploy a job
        :param jobs_details: [($job_name, [{$task_name: $cpu_count}, {$task_name: $mem_limit}])]
        :return: [$($job_name, $task_name, $worker_name, [$core])] + [$waiting_job]
        '''
        scheduling_decision = []
        waiting_decision = []
        for job in jobs_details:
            job_col = self.db[job[0]]
            job_data = mg.find_col(job_col)[0]
            temp_result = []
            for task in job_data['job_info']['tasks']:
                need_waiting = False
                target_node = job_data['job_info']['tasks'][task]['node']

                target_cores = job_data['job_info']['tasks'][task]['cpuset_cpus'].split(',')
                worker_data = self.get_node_info(target_node)
                for core in target_cores:
                    if worker_data['CPUs'][core]:
                        waiting_decision.append(job[0])
                        need_waiting = True
                        break
                if need_waiting:
                    break
                else:
                    temp_result.append((job[0],
                                        job_data['job_info']['tasks'][task]['container_name'],
                                        target_node,
                                        target_cores))
            if len(job_data['job_info']['tasks']) == len(temp_result):
                scheduling_decision.extend(temp_result)
        return scheduling_decision, waiting_decision