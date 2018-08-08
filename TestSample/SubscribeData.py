#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import logging
import zmq_api as zmq
import argparse


def main(address, port):
    logger = doLog()
    socket = zmq.ps_connect(address, port)
    zmq.subscribe_topic(socket, 'number')
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
    parser.add_argument('-p', '--port', type=str, default='3000', help='Port number of publisher container.')
    args = parser.parse_args()
    address = args.address
    port = args.port
    main(address, port)