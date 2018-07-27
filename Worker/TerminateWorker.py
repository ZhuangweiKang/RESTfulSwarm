#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import psutil
import DockerHelper as dh

PROCNAME = 'Worker'
docker_client = dh.setClient()


def kill_worker():
    # leave swarm
    dh.leaveSwarm(docker_client)
    # clean containers
    print(os.popen('docker rm -f $(docker ps -aq)', 'r'))

    # kill process
    for proc in psutil.process_iter():
        if proc.name == PROCNAME:
            proc.kill()


if __name__ == '__main__':
    kill_worker()