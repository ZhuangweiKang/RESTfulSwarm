#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import requests
import argparse
import json
import threading
import time
import ZMQHelper as zmq
import MongoDBHelper as mHelper
from JobManager import Scheduler


gm_addr = None
gm_port = None
db_name = 'RESTfulSwarmDB'
db_client = None
db = None
scheduler = None


# Initialize global manager(Swarm manager node)
def init_GM():
    url = 'http://%s:%s/RESTfulSwarm/GM/init' % (gm_addr, gm_port)
    print(requests.get(url=url).content)


# Create a single task(container)from a Json file
def newTask(data):
    url = 'http://%s:%s/RESTfulSwarm/GM/newtask' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


# Migrate a container
def doMigrate(data):
    # NOTE: We assume destination node always has enough cores to hold the migrated container
    # In other words, Job Manager is a very smart guy.
    dest_node = data['to'].split('@')[0]
    dest_node_info = scheduler.get_node_info(dest_node)
    if dest_node_info is None:
        print('Migration failed, because node %s is not in Swarm environment.')
        return False
    else:
        job_col = db[data['job']]
        workers_col = db['WorkersInfo']
        container = data['container']
        filter_key = 'job_info.tasks.%s.container_name' % container
        target_key = 'job_info.tasks.%s.node' % container
        mHelper.update_doc(job_col, filter_key=filter_key, filter_value=container,
                           target_key=target_key, target_value=dest_node)

        # get cores id from source node
        job_info = list(job_col.find({}))[0]
        cores = job_info['job_info']['tasks'][container]['cpuset_cpus']
        cores = cores.split(',')
        mem_limits = job_info['job_info']['tasks'][container]['mem_limit']
        mem_limits = int(mem_limits.split()[0])

        # get available cores from destination node
        free_cores = []
        for core in dest_node_info['CPUs'].keys():
            if len(free_cores) == len(cores):
                break
            if dest_node_info['CPUs'][core] is False:
                free_cores.append(core)

        # update cpuset_cpus in job info collection
        target_key = 'job_info.tasks.%s.cpuset_cpus' % container
        mHelper.update_doc(job_col, filter_key=filter_key, filter_value=container,
                           target_key=target_key, target_value=','.join(free_cores))

        # mark those cores in source node as free
        src_node = scheduler.find_container(data['container'])
        for core in cores:
            target_key = 'CPUs.%s' % core
            mHelper.update_doc(workers_col, 'hostname', src_node, target_key, False)

        # mark those cores in new node as busy
        for core in free_cores:
            target_key = 'CPUs.%s' % core
            mHelper.update_doc(workers_col, 'hostname', dest_node, target_key, True)

        src_worker_info = list(workers_col.find({'hostname': src_node}))[0]
        src_free_mem = int(src_worker_info['MemFree'].split()[0])
        new_free_mem = int(dest_node_info['MemFree'].split()[0])

        update_old_mem = str(src_free_mem + mem_limits) + ' kB'
        update_new_mem = str(new_free_mem - mem_limits) + ' kB'

        mHelper.update_doc(workers_col, 'hostname', src_node, 'MemFree', update_old_mem)
        mHelper.update_doc(workers_col, 'hostname', dest_node, 'MemFree', update_new_mem)

        data.update({'from': data['from'].split('@')[1]})
        data.update({'to': data['to'].split('@')[1]})

        url = 'http://%s:%s/RESTfulSwarm/GM/requestMigrate' % (gm_addr, gm_port)
        print(requests.post(url=url, json=data).content)

        return True


# Update container resources(cpu & mem)
def updateContainer(data):
    # Update Job Info collection
    new_cpu = data['cpuset_cpus']
    new_mem = data['mem_limits']
    node = data['node']
    container_name = data['container']
    job = data['job']

    job_col = db[job]
    filter_key = 'job_info.tasks.%s.container_name' % container_name
    filter_value = container_name

    # get current cpu info
    container_info = list(job_col.find({}))[0]
    current_cpu = container_info['job_info']['tasks'][container_name]['cpuset_cpus']
    current_mem = container_info['job_info']['tasks'][container_name]['mem_limit']

    # update cpu
    target_key = 'job_info.tasks.%s.cpuset_cpus' % container_name
    target_value = new_cpu
    mHelper.update_doc(job_col, filter_key, filter_value, target_key, target_value)

    # update memory
    target_key = 'job_info.tasks.%s.mem_limits' % container_name
    target_value = new_mem
    mHelper.update_doc(job_col, filter_key, filter_value, target_key, target_value)

    # Update WorkersInfo Collection
    # update cpu
    workersInfo_col = db['WorkersInfo']
    current_cpu = current_cpu.split(',')
    for core in current_cpu:
        key = 'CPUs.%s' % core
        mHelper.update_doc(workersInfo_col, 'hostname', node, key, True)
    new_cpu = new_cpu.split(',')
    for core in new_cpu:
        key = 'CPUs.%s' % core
        mHelper.update_doc(workersInfo_col, 'hostname', node, key, False)

    # update memory, and we assuming memory unit is always kB
    current_mem = int(current_mem.split()[0])
    worker_info = list(workersInfo_col.find({'hostname': node}))[0]
    free_mem = int(worker_info['MemFree'].split()[0])
    current_mem = free_mem + current_mem
    new_mem = int(new_mem.split()[0])
    current_mem -= new_mem
    current_mem = str(current_mem) + ' kB'
    mHelper.update_doc(workersInfo_col, 'hostname', node, 'MemFree', current_mem)

    url = 'http://%s:%s/RESTfulSwarm/GM/requestUpdateContainer' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


# Leave Swarm
def leaveSwarm(hostname):
    col_names = mHelper.get_all_cols(db)
    cols = []
    for col_name in col_names:
        cols.append(mHelper.get_col(db, col_name))
    mHelper.update_tasks(cols, hostname)

    workers_info_col = db['WorkersInfo']
    # remove the node(document) from WorkersInfo collection
    mHelper.delete_document(workers_info_col, 'hostname', hostname)

    data = {'hostname': hostname}
    url = 'http://%s:%s/RESTfulSwarm/GM/requestLeave' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


def dumpContainer(data):
    url = 'http://%s:%s/RESTfulSwarm/GM/checkpointCons' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


def newJobNotify(manager_addr, manager_port):
    socket = zmq.csBind(port='2990')
    while True:
        msg = socket.recv_string()
        socket.send_string('Ack')
        msg = msg.split()

        # Note: read db, parse job resources request
        job_col_name = msg[1]
        job_col = mHelper.get_col(db, job_col_name)
        col_data = list(mHelper.find_col(job_col))[0]

        core_requests = []
        mem_requests = []
        for task in col_data['job_info']['tasks']:
            core_requests.append(task['cpu_count'])
            mem_requests.append(task['mem_limit'])

        # !!! Assuming we have enough capacity to hold any job
        # check resources
        res_check = scheduler.check_resources(core_request=core_requests, mem_request=mem_requests)
        if res_check is not None:
            scheduler.update_job_info(job_col_name, res_check)
            # update WorkersInfo collection
            scheduler.update_workers_info(res_check)
            url = 'http://%s:%s/RESTfulSwarm/GM/requestNewJob' % (manager_addr, manager_port)
            print(requests.post(url=url, json=col_data).content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-ga', '--gaddr', type=str, help='Global manager node address.')
    parser.add_argument('-gp', '--gport', type=str, help='Global manager node port number.')
    parser.add_argument('-ma', '--maddr', type=str, help='MongoDB node address.')
    parser.add_argument('-mp', '--mport', type=str, help='MongoDB port number.')
    args = parser.parse_args()
    gm_addr = args.gaddr
    gm_port = args.gport

    mongo_addr = args.mmaddr
    mongo_port = args.mport
    db_client = mHelper.get_client(mongo_addr, mongo_port)
    db = mHelper.get_db(db_client, db_name)
    scheduler = Scheduler.Scheduler(db, 'WorkersInfo')

    fe_notify_thr = threading.Thread(target=newJobNotify, args=(mongo_addr, mongo_port, ))
    fe_notify_thr.setDaemon(True)
    fe_notify_thr.start()

    while True:
        print('--------------RESTfulSwarmLiveMigration Menu--------------')
        print('1. Init Swarm')
        print('2. Create Task(one container)')
        print('3. Check point a group containers')
        print('4. Migrate Container')
        print('5. Create Job(a group of containers)')
        print('6. Update Container')
        print('7. Leave Swarm')
        print('8. Describe Workers')
        print('9. Describe Manager')
        print('10. Exit')
        try:
            get_input = int(input('Please enter your choice: '))
            if get_input == 1:
                init_GM()
            elif get_input == 2:
                json_path = input('Task Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                newTask(data)
            elif get_input == 3:
                json_path = input('Checkpoint Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                dumpContainer(data)
            elif get_input == 4:
                json_path = input('Migration Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                doMigrate(data)
            elif get_input == 5:
                migrate_json = input('Group migration Json file: ')
                with open(migrate_json, 'r') as f:
                    data = json.load(f)
                for item in data:
                    doMigrate(data)
            elif get_input == 6:
                json_path = input('New resource configuration Json file:')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                updateContainer(data)
            elif get_input == 7:
                hostname = input('Node hostname: ')
                leaveSwarm(hostname)
            elif get_input == 8:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/RESTfulSwarm/GM/%s/describeWorker' % (gm_addr, gm_port, hostname)
                print(requests.get(url=url).content.decode('utf-8'))
            elif get_input == 9:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/RESTfulSwarm/GM/%s/describeManager' % (gm_addr, gm_port, hostname)
                print(requests.get(url=url).content.decode('utf-8'))
            elif get_input == 10:
                print('Thanks for using RESTfulSwarmLM, bye.')
                break
        except ValueError as er:
            print(er)