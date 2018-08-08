#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import math
import json
import random
import time

import utl
from Client.StressClient import StressClient


class BurstyStressClient(StressClient):
    def __init__(self, __lambda):
        super(StressClient, self).__init__()
        self.__lambda = __lambda

    def feed_func(self, time_stamp):
        return math.ceil(random.expovariate(self.__lambda))

    @staticmethod
    def main(session_id):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
        # args = parser.parse_args()
        # fe_address = args.address

        os.chdir('/home/%s/RESTfulSwarmLM/Client' % utl.get_username())

        try:
            json_path = 'BurstyStressClientInfo.json'
            with open(json_path, 'r') as f:
                data = json.load(f)
            client = BurstyStressClient(__lambda=data['lambda'])
            client.feed_jobs(session_id)
        except Exception as ex:
            print(ex)

        os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    session = str(int(time.time()))
    BurstyStressClient.main(session)