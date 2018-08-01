#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import time
import utl
from Client.StressClient import StressClient


class IncrementalStressClient(StressClient):
    def __init__(self, coefficient, constant):
        super(StressClient, self).__init__()
        self.coefficient = coefficient
        self.constant = constant

    def feed_func(self, time_stamp):
        return self.coefficient * time_stamp + self.constant


def main(session_id):
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    # parser.add_argument('-p', '--port', type=str, default='5001', help='Front end node port number.')
    # args = parser.parse_args()
    # fe_addr = args.address
    # fe_port = args.port

    os.chdir('/home/%s/RESTfulSwarmLM/Client' % utl.getUserName())

    try:
        json_path = 'IncrementalStressClientInfo.json'
        with open(json_path, 'r') as f:
            data = json.load(f)
        client = IncrementalStressClient(coefficient=data['coefficient'], constant=data['constant'])
        client.init_fields()
        client.feed_jobs(session_id)
    except ValueError as er:
        print(er)

    os.chdir('/home/%s/RESTfulSwarmLM/ManagementEngine' % utl.getUserName())


if __name__ == '__main__':
    _session_id = str(int(time.time()))
    main(session_id=_session_id)