#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import json
import time

import utl
from Client.StressClient import StressClient


class IncrementalStressClient(StressClient):
    def __init__(self, __coefficient, __constant):
        super(StressClient, self).__init__()
        self.__coefficient = __coefficient
        self.__constant = __constant

    def feed_func(self, time_stamp):
        return self.__coefficient * time_stamp + self.__constant

    @staticmethod
    def main(session_id):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
        # args = parser.parse_args()
        # fe_address = args.address

        os.chdir('/home/%s/RESTfulSwarmLM/Client' % utl.get_username())

        try:
            json_path = 'IncrementalStressClientInfo.json'
            with open(json_path, 'r') as f:
                data = json.load(f)
            client = IncrementalStressClient(__coefficient=data['coefficient'], __constant=data['constant'])
            client.feed_jobs(session_id)
        except ValueError as er:
            print(er)

        os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    session = str(int(time.time()))
    IncrementalStressClient.main(session)