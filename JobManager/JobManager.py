#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import requests
import argparse
import json
import threading
import ZMQHelper as zmq
import MongoDBHelper as mHelper

gm_addr = None
gm_port = None


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


# Update container resources(cpu & mem)
def updateContainer(data):
    url = 'http://%s:%s/RESTfulSwarm/GM/requestUpdateContainer' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


# Leave Swarm
def leaveSwarm(hostname):
    data = {'hostname': hostname}
    url = 'http://%s:%s/RESTfulSwarm/GM/requestLeave' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


def dumpContainer(data):
    url = 'http://%s:%s/RESTfulSwarm/GM/checkpointCons' % (gm_addr, gm_port)
    print(requests.post(url=url, json=data).content)


def newJobNotify(manager_addr, manager_port, mongo_addr, mongo_port):
    socket = zmq.csBind(port='2990')
    while True:
        msg = socket.recv_string()
        socket.send_string('Ack')
        msg = msg.split()
        db = msg[0]
        col = msg[1]
        m_client = mHelper.get_client(mongo_addr, mongo_port)
        m_db = mHelper.get_db(m_client, db)
        col_data = mHelper.get_col(m_db, col).find_one()
        url = 'http://%s:%s/RESTfulSwarm/GM/requestNewJob' % (manager_addr, manager_port)
        print(requests.post(url=url, json=col_data).content)

        # update job status in db
        m_db[col].update_one({"status": "deployed"})


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

    fe_notify_thr = threading.Thread(target=newJobNotify, args=(mongo_addr, mongo_port, ))
    fe_notify_thr.setDaemon(True)
    fe_notify_thr.start()

    while True:
        print('--------------RESTfulSwarmLiveMigration Menu--------------')
        print('1. Init Swarm')
        print('2. Create Task(one container)')
        print('3. Check point a group containers')
        print('4. Migrate Container')
        print('5. Migrate a group of containers')
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