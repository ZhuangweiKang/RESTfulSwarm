#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import zmq


# C/S socket
def cs_connect(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    connect_str = 'tcp://%s:%s' % (address, port)
    socket.connect(connect_str)
    return socket


def cs_bind(port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:%s' % port)
    return socket


# Pub/Sub Socket
def ps_connect(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    connect_str = 'tcp://%s:%s' % (address, port)
    socket.connect(connect_str)
    return socket


def ps_bind(port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://*:%s' % port)
    return socket


def subscribe_topic(socket, topic):
    socket.subscribe(topic)


def unsubscribe_topic(socket, topic):
    socket.unsubscribe(topic)


def publish(socket, msg):
    socket.send_string(msg)