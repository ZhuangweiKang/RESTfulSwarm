#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import requests
import argparse
import json

fe_addr = None
fe_port = None


def newJob(data):
    url = 'http://%s:%s/RESTfulSwarm/FE/requestNewJob' % (fe_addr, fe_port)
    print(requests.post(url=url, json=data).content)


if __name__ == '__main__':
    '''
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    parser.add_argument('-p', '--port', type=str, default='5000', help='Front end node port number.')
    args = parser.parse_args()
    fe_addr = args.address
    fe_port = args.port
    '''
    with open('ClientInit.json') as f:
        data = json.load(f)
    fe_addr = data['address']
    fe_port = data['port']

    while True:
        try:
            json_path = input('Job Info Json file path:')
            with open(json_path, 'r') as f:
                data = json.load(f)
            newJob(data)
        except ValueError as er:
            print(er)