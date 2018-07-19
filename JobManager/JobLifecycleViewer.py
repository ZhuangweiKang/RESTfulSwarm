#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import argparse
import MongoDBHelper as mHelper


def display(data):
    submit_time = data['submit_time']
    start_time = data['start_time']
    end_time = data['end_time']
    print('Submit time: %f' % submit_time)
    print('Start execution time: %f' % start_time)
    print('End execution time: %f' % end_time)
    print('Waiting time: %f seconds.' % start_time - submit_time)
    print('Execution time: %f seconds.' % end_time - start_time)
    print('Lifecycle: %s seconds.' % end_time - submit_time)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-j', '--job', type=str, help='Job name.')
    args = parser.parse_args()
    job_name = args.job

    client = mHelper.get_client(usr='admin', pwd='kzw', address='129.59.107.139')
    db = mHelper.get_db(client, 'RESTfulSwarmDB')
    col = mHelper.get_col(db, job_name)
    display(mHelper.find_col(col))