#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import psutil

import docker_api as dh
import utl

PROCNAME = 'python3'
docker_client = dh.set_client()


def kill_worker():
    os.chdir('/home/%s/RESTfulSwarmLM/Worker' % utl.get_username())

    # leave swarm
    dh.leave_swarm(docker_client)
    # clean containers
    print(os.popen('docker rm -f $(docker ps -aq)', 'r'))

    # kill process
    for proc in psutil.process_iter():
        if proc.name() == PROCNAME and proc.pid != os.getpid():
            proc.kill()


if __name__ == '__main__':
    kill_worker()