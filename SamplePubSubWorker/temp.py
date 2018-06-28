#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang


import zmq


def producer(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.PUB)
    connect_str = 'pgm://%s:%s' % (address, port)
    socket.connect(connect_str)
    return socket


def consumer(address, port):
    context = zmq.Context()
    socket = context.socket(zmq.SUB)
    connect_str = 'pgm://%s:%s' % (address, port)
    socket.connect(connect_str)
    socket.setsocket(zmq.SUBSCRIBE, 'num')
    return socket
