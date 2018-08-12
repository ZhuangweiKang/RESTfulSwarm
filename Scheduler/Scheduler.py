#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

from abc import ABCMeta, abstractmethod
import mongodb_api as mg
import SystemConstants


class Scheduler(object):
    __metaclass__ = ABCMeta

    def __init__(self, db):
        self.db = db
        self.workers_col = mg.get_col(self.db, SystemConstants.WorkersInfo)
        self.workers_resource_col = mg.get_col(self.db, SystemConstants.WorkersResourceInfo)

    @abstractmethod
    def cores_scheduling_algorithm(self, jobs_details, free_cores):
        pass

    @abstractmethod
    def schedule_resources(self, jobs_details):
        pass

    def collect_free_cores(self):
        # get all free cores from every worker node
        available_workers = {}
        for worker in list(self.workers_col.find({})):
            available_workers.update({worker['hostname']: []})
            for cpu in worker['CPUs'].keys():
                if worker['CPUs'][cpu] is False:
                    available_workers[worker['hostname']].append(cpu)
        return available_workers

    def update_job_info(self, schedule):
        for item in schedule:
            job_col = mg.get_col(self.db, item[0])
            job_filter = 'job_info.tasks.%s.container_name' % item[1]

            # add node filed
            target = 'job_info.tasks.%s.node' % item[1]
            mg.update_doc(job_col, job_filter, item[1], target, item[2])

            # add cpuset_cpus field
            target = 'job_info.tasks.%s.cpuset_cpus' % item[1]
            mg.update_doc(job_col, job_filter, item[1], target, ','.join(item[3]))

    def update_workers_info(self, schedule):
        for item in schedule:
            for core in item[3]:
                target = 'CPUs.%s' % str(core)
                mg.update_doc(self.workers_col, 'hostname', item[2], target, True)

    # update worker resource collection
    def update_worker_resource_info(self, schedule):
        hosts = set(list(map(lambda item: item[2], schedule)))
        for host in hosts:
            mg.update_workers_resource_col(self.workers_col, host, self.workers_resource_col)

    def find_container(self, container_name):
        # '''
        # Find the worker node the container is locating
        # :param container_name:
        # :return: return node hostname
        # '''
        collections = self.db.collection_names(include_system_collections=False)
        temp = []
        for collection in collections:
            filter_key = 'job_info.tasks.%s.container_name' % container_name
            jobs_col = mg.get_col(self.db, collection)
            temp = list(jobs_col.find({filter_key: container_name}))
            if len(temp) != 0:
                break
        return temp[0]['job_info']['tasks'][container_name]['node'] if len(temp) != 0 else None

    def get_node_info(self, node_name):
        # '''
        # Get node information by node_name
        # :param node_name:
        # :return: A python dict if the node is available, or None if not
        # '''
        try:
            return list(self.workers_col.find({'hostname': node_name}))[0]
        except Exception:
            return None