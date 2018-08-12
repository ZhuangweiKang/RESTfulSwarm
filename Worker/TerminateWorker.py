#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
import traceback
import psutil

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import docker_api as dh
import utl

PROCNAME = 'python3'
docker_client = dh.set_client()


def kill_worker():
    os.chdir('/home/%s/RESTfulSwarm/Worker' % utl.get_username())
    if len(dh.list_containers(docker_client)) != 0:
        # leave swarm
        dh.leave_swarm(docker_client)
        # clean containers
        os.system('sudo docker rm -f $(docker ps -aq)')

    # kill process
    for proc in psutil.process_iter():
        if proc.name() == PROCNAME and proc.pid != os.getpid():
            proc.kill()


if __name__ == '__main__':
    kill_worker()