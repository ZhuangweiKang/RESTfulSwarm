#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import logging
import ZMQHelper as zmq
import argparse


def main(address, port):
    logger = doLog()
    socket = zmq.connect(address, port)
    zmq.subscribeTopic(socket, 'number')
    while True:
        data = socket.recv()
        number = data.split()
        logger.info(number)


def doLog():
    logger = logging.getLogger('SubLogger')
    logger.setLevel(logging.DEBUG)

    fl = logging.FileHandler('/home/Sub.log')
    fl.setLevel(logging.DEBUG)

    cl = logging.StreamHandler()
    cl.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fl.setFormatter(formatter)
    cl.setFormatter(formatter)

    logger.addHandler(fl)
    logger.addHandler(cl)

    return logger


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='IP address of publisher container.')
    parser.add_argument('-p', '--port', type=str, help='Port number of publisher container.')
    args = parser.parse_args()
    address = args.address
    port = args.port
    main(address, port)