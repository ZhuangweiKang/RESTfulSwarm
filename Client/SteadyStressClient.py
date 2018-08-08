#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import json
import time

import utl
from Client.StressClient import StressClient


class SteadyStressClient(StressClient):
    def __init__(self, __steady_constant):
        super(StressClient, self).__init__()
        self.__steady_constant = __steady_constant

    def feed_func(self, time_stamp):
        return self.__steady_constant

    @staticmethod
    def main(session_id):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
        # args = parser.parse_args()
        # fe_address = args.address

        os.chdir('/home/%s/RESTfulSwarmLM/Client' % utl.get_username())

        try:
            json_path = 'SteadyStressClientInfo.json'
            with open(json_path, 'r') as f:
                data = json.load(f)
            client = SteadyStressClient(__steady_constant=data['feed_constant'])
            client.feed_jobs(session_id)
        except ValueError as er:
            print(er)

        os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    session = str(int(time.time()))
    SteadyStressClient.main(session)