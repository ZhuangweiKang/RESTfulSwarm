#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import random
import utl
import math
from Client.StressClient import StressClient


class BurstyStressClient(StressClient):
    def __init__(self, lmda):
        super(StressClient, self).__init__()
        self.lmda = lmda

    def feed_func(self, time_stamp):
        return math.ceil(random.expovariate(self.lmda))


def main():
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    # parser.add_argument('-p', '--port', type=str, default='5001', help='Front end node port number.')
    # args = parser.parse_args()
    # fe_addr = args.address
    # fe_port = args.port
    os.chdir('/home/%s/RESTfulSwarmLM/Client' % utl.getUserName())
    try:
        json_path = 'BurstyStressClientInfo.json'
        with open(json_path, 'r') as f:
            data = json.load(f)
        client = BurstyStressClient(lmda=data['lambda'])
        client.init_fields()
        client.feed_jobs()
    except ValueError as er:
        print(er)

    os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.getUserName())


if __name__ == '__main__':
    main()