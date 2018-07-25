#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import json
from Client.Client import StressClient


class SteadyStressClient(StressClient):
    def __init__(self, subnet, image_name, task_count, task_cores, task_mem, time_interval, feed_constant):
        super(StressClient).__init__(subnet=subnet,
                                     image_name=image_name,
                                     task_cores=task_cores,
                                     task_mem=task_mem,
                                     task_count=task_count,
                                     time_interval=time_interval)
        self.feed_constant = feed_constant

    def feed_func(self, time_stamp):
        return self.feed_constant


if __name__ == '__main__':
    # parser = argparse.ArgumentParser()
    # parser.add_argument('-a', '--address', type=str, help='Front end node address.')
    # parser.add_argument('-p', '--port', type=str, default='5000', help='Front end node port number.')
    # args = parser.parse_args()
    # fe_addr = args.address
    # fe_port = args.port

    try:
        json_path = 'SteadyStressClientInfo.json'
        with open(json_path, 'r') as f:
            data = json.load(f)
        client = SteadyStressClient(subnet=data['subnet'],
                                    image_name=data['image_name'],
                                    task_count=data['task_count'],
                                    task_cores=data['task_cores'],
                                    task_mem=data['task_mem'],
                                    time_interval=data['time_interval'],
                                    feed_constant=data['feed_constant'])
        client.feed_jobs()
    except ValueError as er:
        print(er)