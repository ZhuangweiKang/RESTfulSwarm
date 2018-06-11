#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import requests
import argparse
import json

manager_addr = None
manager_port = None


def init_manager(network, subnet):
    url = 'http://%s:%s/SwarmLMGM/manager/init' % (manager_addr, manager_port)
    data = {'network': network, 'subnet': subnet}
    print(requests.post(url=url, json=data).content)


def newContainer(data):
    url = 'http://%s:%s/SwarmLMGM/worker/requestNewContainer' % (manager_addr, manager_port)
    print(requests.post(url=url, json=data).content)


def doMigrate(container, src, dst):
    data = {'container': container,
            'from': src,
            'to': dst
    }
    url = 'http://%s:%s/SwarmLMGM/worker/requestMigrate' % (manager_addr, manager_port)
    print(requests.post(url=url, json=data).content)


def updateContainer(node_name, container_name, cpuset_cpus, mem_limits):
    data = {'node': node_name,
            'container_name': container_name,
            'cpuset_cpus': cpuset_cpus,
            'mem_limits': mem_limits}
    url = 'http://%s:%s/SwarmLMGM/worker/requestUpdateContainer' % (manager_addr, manager_port)
    print(requests.post(url=url, json=data).content)


def leaveSwarm(hostname):
    data = {'hostname': hostname}
    url = 'http://%s:%s/SwarmLMGM/worker/requestLeave' % (manager_addr, manager_port)
    print(requests.post(url=url, json=data).content)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Manager node address.')
    parser.add_argument('-p', '--port', type=str, help='Manager node port number.')
    args = parser.parse_args()
    manager_addr = args.address
    manager_port = args.port
    while True:
        print('--------------RESTfulSwarmLiveMigration Menu--------------')
        print('1. Init Swarm')
        print('2. New Container')
        print('3. Migrate Container')
        print('4. Update Container')
        print('5. Leave Swarm')
        print('6. Describe Workers')
        print('7. Describe Manager')
        print('8. Exit')
        try:
            get_input = int(input('Please enter your choice: '))
            if get_input == 1:
                network = input('Network name: ')
                subnet = input('Subnet in CIDR format: ')
                init_manager(network=network, subnet=subnet)
            elif get_input == 2:
                json_path = input('Json file path: ')
                with open(json_path, 'r') as f:
                    data = json.load(f)
                newContainer(data)
            elif get_input == 3:
                container = input('Container name: ')
                src = input('From: ')
                dst = input('To: ')
                doMigrate(container, src, dst)
            elif get_input == 4:
                node_name = input('Node hostname: ')
                container_name = input('Container name: ')
                cpuset_cpus = input('CPU set cpus: ')
                mem_limits = input('Memory limit: ')
                updateContainer(node_name, container_name, cpuset_cpus, mem_limits)
            elif get_input == 5:
                hostname = input('Node hostname: ')
                leaveSwarm(hostname)
            elif get_input == 6:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/SwarmLMGM/worker/%s/describeWorker' % (manager_addr, manager_port, hostname)
                print(json.loads(str(requests.get(url=url).content)))
            elif get_input == 7:
                hostname = input('Node hostname: ')
                url = 'http://%s:%s/SwarmLMGM/manager/%s/describeManager' % (manager_addr, manager_port, hostname)
                print(json.loads(str(requests.get(url=url).content)))
            elif get_input == 8:
                print('Thanks for using RESTfulSwarmLM, bye.')
                break
        except ValueError as er:
            print(er)