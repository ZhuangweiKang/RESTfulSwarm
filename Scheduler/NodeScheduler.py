#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


from Scheduler.Scheduler import Scheduler


class NodeScheduler(Scheduler):
    def __init__(self, db):
        super(NodeScheduler, self).__init__(db)

    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass

    def schedule_resources(self, jobs_details):
        # '''
        # Check if we have enough capacity to deploy a job
        # :param jobs_details: [($job_name, [{$task_name: $cpu_count}, {$task_name: $mem_limit}, [$node], [$cpuset_cpus]])]
        # :return: [$($job_name, $task_name, $worker_name, [$core])] + [$waiting_job]
        # '''
        scheduling_decision = []
        waiting_decision = []
        for job in jobs_details:
            temp_result = []
            for i in range(len(job[1][0])):
                need_waiting = False
                target_node = job[1][2][i]
                target_cores = job[1][3][i].split(',')
                worker_data = self.get_node_info(target_node)
                for core in target_cores:
                    if worker_data['CPUs'][core]:
                        waiting_decision.append(job[0])
                        need_waiting = True
                        break
                if need_waiting:
                    break
                else:
                    temp_result.append((job[0], list(job[1][0].keys())[i], target_node, target_cores))
            if len(job[1][0]) == len(temp_result):
                scheduling_decision.extend(temp_result)
        return scheduling_decision, waiting_decision