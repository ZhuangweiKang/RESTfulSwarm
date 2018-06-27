#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import zmq


# -------- C/S socket
def csConnect(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    connect_str = 'tcp://%s:%s' % (address, port)
    socket.connect(connect_str)
    return socket


def csBind(port):
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind('tcp://*:%s' % port)
    return socket


# -------- Pub/Sub Socket
def connect(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    connect_str = 'tcp://%s:%s' % (address, port)
    socket.connect(connect_str)
    return socket


def bind(port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    socket.bind('tcp://*:%s' % port)
    return socket


def subscribeTopic(socket, topic):
    socket.subscribe(topic)


def unsubscribeTopic(socket, topic):
    socket.unsubscribe(topic)


def publish(socket, msg):
    socket.send_string(msg)


def multicast_producer(broadcast_addr, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    connect_str = 'epgm://%s:%s' % (broadcast_addr, port)
    socket.connect(connect_str)
    return socket


def multicast_consumer(broadcast_addr, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    connect_str = 'epgm://%s:%s' % (broadcast_addr, port)
    socket.connect(connect_str)
    return socket