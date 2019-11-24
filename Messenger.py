#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import zmq


class Messenger(object):
    def __init__(self, messenger_type, **kwargs):
        if messenger_type == 'C/S':
            self.agent = self.cs_connect(address=kwargs['address'], port=kwargs['port']) if len(kwargs) == 2 \
                else self.cs_bind(port=kwargs['port'])
        elif messenger_type == 'Pub/Sub':
            self.agent = self.ps_connect(address=kwargs['address'], port=kwargs['port']) if len(kwargs) == 2 \
                else self.ps_bind(port=kwargs['port'])

    def send(self, prompt='', content=''):
        msg = '%s %s' % (prompt, content)
        self.agent.send_string(msg)
        self.agent.recv_string()

    def receive(self, feedback):
        msg = self.agent.recv_string()
        self.agent.send_string(feedback)
        return msg

    def subscribe(self):
        return self.agent.recv_string()

    def publish(self, msg):
        self.agent.send_string(msg)

    def subscribe_topic(self, topic):
        self.agent.subscribe(topic)

    def unsubscribe_topic(self, topic):
        self.agent.unsubscribe(topic)

    @staticmethod
    def cs_connect(address, port):
        context = zmq.Context()
        socket = context.socket(zmq.REQ)
        connect_str = 'tcp://%s:%s' % (address, port)
        socket.connect(connect_str)
        return socket

    @staticmethod
    def cs_bind(port):
        context = zmq.Context()
        socket = context.socket(zmq.REP)
        socket.bind('tcp://*:%s' % port)
        return socket

    @staticmethod
    def ps_connect(address, port):
        context = zmq.Context()
        socket = context.socket(zmq.SUB)
        connect_str = 'tcp://%s:%s' % (address, port)
        socket.connect(connect_str)
        return socket

    @staticmethod
    def ps_bind(port):
        context = zmq.Context()
        socket = context.socket(zmq.PUB)
        socket.bind('tcp://*:%s' % port)
        return socket