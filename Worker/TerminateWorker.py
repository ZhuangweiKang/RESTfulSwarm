#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psutil
import DockerHelper as dh
import utl

PROCNAME = 'python3'
docker_client = dh.setClient()


def kill_worker():
    os.chdir('/home/%s/RESTfulSwarmLM/Worker' % utl.getUserName())

    # leave swarm
    dh.leaveSwarm(docker_client)
    # clean containers
    print(os.popen('docker rm -f $(docker ps -aq)', 'r'))

    # kill process
    for proc in psutil.process_iter():
        if proc.name() == PROCNAME and proc.pid != os.getpid():
            proc.kill()


if __name__ == '__main__':
    kill_worker()