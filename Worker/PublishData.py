#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import time
import ZMQHelper as zmq

'''
GlobalManager publishes numbers like a busybox container
'''
def main():
    port = '3000'
    topic = 'number'
    socket = zmq.bind(port)
    i = 0
    while True:
        socket.send('%s %s' % (topic, str(i)))
        print('Send numeric string: %d' % i)
        time.sleep(2)
        i += 1


if __name__ == '__main__':
    main()