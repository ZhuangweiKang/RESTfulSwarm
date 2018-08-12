#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
import traceback
import json
import time

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
import utl
from Client.StressClient import StressClient


class SteadyStressClient(StressClient):
    def __init__(self, steady_constant):
        super(SteadyStressClient, self).__init__()
        self.__steady_constant = steady_constant

    def feed_func(self, time_stamp):
        return self.__steady_constant

    @staticmethod
    def main(session_id):
        # parser = argparse.ArgumentParser()
        # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
        # args = parser.parse_args()
        # fe_address = args.address

        os.chdir('/home/%s/RESTfulSwarm/Client' % utl.get_username())

        try:
            json_path = 'SteadyStressClientInfo.json'
            with open(json_path, 'r') as f:
                data = json.load(f)
            client = SteadyStressClient(steady_constant=data['feed_constant'])
            client.feed_jobs(session_id)
        except Exception:
            traceback.print_exc(file=sys.stdout)

        os.chdir('/home/%s/RESTfulSwarm/ManagementEngine' % utl.get_username())


if __name__ == '__main__':
    session = str(int(time.time()))
    SteadyStressClient.main(session)