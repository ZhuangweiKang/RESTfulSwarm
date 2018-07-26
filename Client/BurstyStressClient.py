#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import json
import random
import math
from Client.Client import StressClient


class BurstyStressClient(StressClient):
    def __init__(self, lmda):
        super(StressClient).__init__()
        self.lmda = lmda

    def feed_func(self, time_stamp):
        return math.ceil(random.expovariate(self.lmda))


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    # parser.add_argument('-p', '--port', type=str, default='5001', help='Front end node port number.')
    # args = parser.parse_args()
    # fe_addr = args.address
    # fe_port = args.port

    try:
        json_path = 'BurstyStressClientInfo.json'
        with open(json_path, 'r') as f:
            data = json.load(f)
        client = BurstyStressClient(lmda=data['lambda'])
        client.feed_jobs()
    except ValueError as er:
        print(er)