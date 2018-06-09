import requests

manager_addr = None
manager_port = None


def init_manager(network, subnet):
    url = 'http://%s:%s/SwarmLMGM/manager/init' % (manager_addr, manager_port)
    data = {'network': network, 'subnet': subnet}
    requests.post(url=url, data=data)


def newContainer(image, name, detach=True, network=None, command=None, cpuset_cpus=None, mem_limit=None, ports=None, volumes=None):
    data = {
        'image': image,
        'name': name,
        'detach': detach,
        'network': network,
        'cpuset_cpus': cpuset_cpus,
        'mem_limit': mem_limit,
        'ports': ports,
        'volumes': volumes,
        'command': command
    }
    url = 'http://%s:%s/SwarmLMGM/worker/requestNewContainer' % (manager_addr, manager_port)
    requests.post(url=url, data=data)


def doMigrate(container, src, dst):
    data = {'container': container,
            'from': src,
            'to': dst
    }
    url = 'http://%s:%s/SwarmLMGM/worker/requestMigrate' % (manager_addr, manager_port)
    requests.post(url=url, data=data)


def updateContainer(node_name, container_name, cpuset_cpus, mem_limit):
    data = {'node': node_name,
            'container_name': container_name,
            'cpuset_cpus': cpuset_cpus,
            'mem_limit': mem_limit}
    url = 'http://%s:%s/SwarmLMGM/worker/requestUpdateContainer' % (manager_addr, manager_port)
    requests.post(url=url, data=data)


def leaveSwarm(hostname):
    data = {'hostname': hostname}
    url = 'http://%s:%s/SwarmLMGM/worker/requestLeave' % (manager_addr, manager_port)
    requests.post(url=url, data=data)


if __name__ == '__main__':
    print('--------------RESTfulSwarmLiveMigration Menu--------------')
    print('1. Init Swarm')
    print('2. New Container')
    print('3. Migrate Container')
    print('4. ')
    get_input = input('Please enter your choice: ')