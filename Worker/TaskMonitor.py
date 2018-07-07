#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import DockerHelper as dHelper
import ZMQHelper as zmq
import utl
import time


class TaskMonitor:
    def monitor(self, discovery_addr, discovery_port='4000', frequency=20):
        time_flag = time.time()
        client = dHelper.setClient()
        time.sleep(frequency)
        hostname = utl.getHostName()
        socket = zmq.csConnect(discovery_addr, discovery_port)
        while True:
            events = client.events(since=time_flag, until=time.time(), decode=True)
            msgs = []
            for event in events:
                if event['Type'] == 'container' and event['status'] == 'stop':
                    msg = hostname + ' ' + event['Actor']['Attributes']['name']
                    msgs.append(msg)

            # Notify discovery block to update MongoDB
            for msg in msgs:
                socket.send_string(msg)
                socket.recv_string()

            time_flag = time.time()
            time.sleep(frequency)