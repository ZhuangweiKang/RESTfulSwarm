#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
import docker
import docker.errors
import docker.types
import time
import traceback


def set_client():
    return docker.from_env()


def build_image(client, path, tag):
    return client.images.build(path=path, tag=tag)


def pull_image(client, repository):
    client.images.pull(repository)


def run_container(client, image, name, detach=True, network=None, command=None, cpuset_cpus=None, mem_limit=None, ports=None, volumes=None, environment=None):
    return client.containers.run(
        image=image,
        name=name,
        detach=detach,
        network=network,
        cpuset_cpus=cpuset_cpus,
        mem_limit=mem_limit,
        ports=ports,
        volumes=volumes,
        command=command,
        environment=environment
    )


def update_container(client, container_name, cpuset_cpus, mem_limit):
    container = get_container(client=client, name=container_name)
    if container is not None:
        container.update(cpuset_cpus=cpuset_cpus, mem_limit=mem_limit)
        return True
    return False


def get_container(client, name):
    try:
        return client.containers.get(name)
    except docker.errors.NotFound:
        return None


def check_image(client, tag):
    images = client.images.list()
    for image in images:
        if tag in image.tags:
            return True
    return False


def check_container(client, container_name):
    try:
        client.containers.get(container_name)
        return True
    except docker.errors.NotFound:
        return False


def delete_container(container):
    container.remove(force=True)


def get_container_id(client, container):
    return client.containers.get(container).id


def checkpoint(checkpoint_name, container_id, leave_running=False):
    print(checkpoint_name, int(round(time.time() * 1000)))
    if leave_running:
        checkpoint_cmd = 'docker checkpoint create --leave-running ' + container_id + ' ' + checkpoint_name
    else:
        checkpoint_cmd = 'docker checkpoint create ' + container_id + ' ' + checkpoint_name
    print(os.popen(checkpoint_cmd, 'r').read())
    print(checkpoint_name, int(round(time.time() * 1000)))


def restore(container_id, checkpoint_dir, checkpoint_name):
    # checkpoint_dir = '/var/lib/docker/containers/%s/checkpoints/' % containerID
    restore_cmd = 'docker start --checkpoint-dir=%s --checkpoint=%s %s' \
                  % (checkpoint_dir, checkpoint_name, container_id)
    print(os.popen(restore_cmd, 'r').read())


def init_swarm(client, advertise_addr):
    client.swarm.init(advertise_addr=advertise_addr)


def join_swarm(client, token, address):
    client.swarm.join(remote_addrs=[address], join_token=token)


def leave_swarm(client):
    try:
        client.swarm.leave(force=True)
    except Exception as ex:
        print(ex)


def get_node_list(client):
    try:
        return client.nodes.list(filters={'role': 'worker'})
    except Exception:
        traceback.print_exc(file=sys.stderr)
        return None


def get_join_token():
    cmd = 'docker swarm join-token worker -q'
    return os.popen(cmd, 'r').read()


def delete_node(node_name):
    cmd = 'docker node rm %s' % node_name
    os.system(cmd)


def create_network(client, name, driver='overlay', attachable=True, subnet=None):
    ipam_pool = docker.types.IPAMPool(subnet=subnet)
    ipam_config = docker.types.IPAMConfig(pool_configs=[ipam_pool])
    client.networks.create(name=name, driver=driver, ipam=ipam_config, attachable=attachable)


def get_container_ip(container_name):
    cmd = 'docker inspect -f \'{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}\' %s' % container_name
    return os.popen(cmd, 'r').read()


def create_container(client, image, name, network=None, command=None):
    client.containers.create(image=image, name=name, detach=True, network=network, command=command)


def commit_container(client, name, repository, image_name, tag='latest'):
    container = get_container(client, name)
    get_tags(client, image_name)
    container.commit(repository=repository, tag=tag)
    client.images.push(repository, tag=tag)
    return repository + ':' + tag


def get_tags(client, image_name):
    tags = client.images.get(image_name).tags
    image = client.images.get(image_name)
    temp = []
    for tag in tags:
        tag = tag.split(':')[1]
        if tag != 'latest':
            temp.append(float(tag))
    max_tag = 0.0 if len(temp) == 0 else max(temp)
    image.tag(repository=image_name, tag=str(max_tag+0.1))
    client.images.push(repository=image_name, tag=str(max_tag+0.1))


def verify_network(client, network):
    # get a list of network objects
    networks = client.networks.list()
    # get a list of network name
    networks = [network.name for network in networks]
    return False if network in networks else True


def check_node_ip(client, node_ip):
    nodes = get_node_list(client)
    for node in nodes:
        if node.attrs['Status']['Addr'] == node_ip:
            return True
    return False


def check_node_hostname(client, host):
    nodes = get_node_list(client)
    for node in nodes:
        if node.attrs['ID'] == host:
            return False
    return True


def get_node_id(client, node_ip):
    nodes = get_node_list(client)
    for node in nodes:
        if node.attrs['Status']['Addr'] == node_ip:
            return node.attrs['ID']
    return None


def get_node_info(client, name):
    try:
        node = client.nodes.get(name)
        return node.attrs
    except Exception as ex:
        print(ex)


def remove_node(name):
    cmd = 'docker node rm -f %s' % name
    print(os.popen(cmd).read())


def prune_network(client, _filter=None):
    return client.networks.prune(filters=_filter)


def rm_networks(client, networks):
    # remove a list of networks
    all_networks = list(filter(lambda nw: nw.name in networks, client.networks.list()))
    [nw.remove() for nw in all_networks]


def list_containers(client):
    return client.containers.list(all=True)