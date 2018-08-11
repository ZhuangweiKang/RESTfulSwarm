#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import time
from Messenger import Messenger


def main():
    port = '3000'
    topic = 'number'
    messenger = Messenger(messenger_type='Pub/Sub', port=port)
    i = 0
    while True:
        messenger.publish('%s %s' % (topic, str(i)))
        print('Send numeric string: %d' % i)
        time.sleep(2)
        i += 1


if __name__ == '__main__':
    main()