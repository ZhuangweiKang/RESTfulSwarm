#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import json
import random
import time

import utl
from Client.StressClient import StressClient


class RandomStressClient(StressClient):
    def __init__(self, __lower_bound, __upper_bound):
        super(StressClient, self).__init__()
        self.__lower_bound = __lower_bound
        self.__upper_bound = __upper_bound

    def feed_func(self, time_stamp):
        return random.randint(self.__lower_bound, self.__upper_bound)

    @staticmethod
    def main(session_id):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
        # args = parser.parse_args()
        # fe_address = args.address

        os.chdir('/home/%s/RESTfulSwarm/Client' % utl.get_username())

        try:
            json_path = 'RandomStressClientInfo.json'
            with open(json_path, 'r') as f:
                data = json.load(f)
            client = RandomStressClient(__lower_bound=data['lower_bound'], __upper_bound=data['upper_bound'])
            client.feed_jobs(session_id)
        except Exception as er:
            print(er)

        os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    session = str(int(time.time()))
    RandomStressClient.main(session)