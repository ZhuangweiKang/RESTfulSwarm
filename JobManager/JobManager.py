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
    url = 'http://%s:%s/RESTfulSwarm/GM/requestMigrate' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)

    time.sleep(1)

    # TODO: update db (node name) --- Need to debug
    update_info = []
    for con in data:
        update_info.append({'job': con['job'], 'container': con['container'], 'node': con['to'].split('@')[0]})

    for item in update_info:
        col = db[item['job']]
        filter_key = 'job_info.tasks.%s.container_name' % item['container']
        filter_value = item['container']
        target_key = 'job_info.tasks.%s.node' % item['container']
        target_value = item['node']
        mHelper.update_doc(col, filter_key, filter_value, target_key, target_value)
        time.sleep(1)


# Update container resources(cpu & mem)
def updateContainer(data):
    url = 'http://%s:%s/RESTfulSwarm/GM/requestUpdateContainer' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)

    # TODO: Update db (cpuset_cpus & mem_limits) --- Need to debug
    new_cpu = data['cpuset_cpus']
    new_mem = data['mem_limits']
    container_name = data['container']
    job = data['job']

    col = mHelper.get_col(db, job)
    filter_key = 'job_info.tasks.%s.container_name' % container_name
    filter_value = container_name

    # update cpu
    target_key = 'job_info.tasks.%s.cpuset_cpus' % container_name
    target_value = new_cpu
    mHelper.update_doc(col, filter_key, filter_value, target_key, target_value)

    # update memory
    target_key = 'job_info.tasks.%s.mem_limits' % container_name
    target_value = new_mem
    mHelper.update_doc(col, filter_key, filter_value, target_key, target_value)


# Leave Swarm
def leaveSwarm(hostname):
    data = {'hostname': hostname}
    url = 'http://%s:%s/RESTfulSwarm/GM/requestLeave' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)

    # TODO: update db (delete all jobs & tasks on the node) --- Need to debug
    col_names = mHelper.get_all_cols(db)
    cols = []
    for col in col_names:
        cols.append(mHelper.get_col(db, col))
    mHelper.update_tasks(cols, hostname)


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
        for task in col_data['job_info']['tasks']:
            core_requests.append(task['cpu_count'])

        # !!! Assuming we have enough capacity to hold any job
        # check resources
        res_check = scheduler.check_resources(core_request=core_requests)
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