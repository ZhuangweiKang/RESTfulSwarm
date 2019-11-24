#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import pymongo as mg
import time


# return a mongodb client
def get_client(usr, pwd, db_name, address='127.0.0.1', port='27017'):
    url = 'mongodb://%s:%s@%s:%s/%s' % (usr, pwd, address, port, db_name)
    return mg.MongoClient(url)


# return a database object
def get_db(client, db_name):
    return client[db_name]


# return a collection cursor object
def get_col(db, col_name):
    return db[col_name]


def find_col(col):
    return list(col.find({}))


def filter_col(col, filter_key, filter_value):
    try:
        return list(col.find({filter_key: filter_value}))[0]
    except Exception:
        return None


def get_all_cols(db):
    return db.list_collection_names()


# delete a collection(a job in my work)
def drop_col(client, db_name, col_name):
    db = client[db_name]
    col = db[col_name]
    col.drop()


def insert_doc(col, data):
    return col.insert_one(data).inserted_id


def update_doc(col, filter_key, filter_value, target_key, target_value):
    return col.update_one({filter_key: filter_value}, {"$set": {target_key: target_value}})


def delete_document(col, filter_key, filter_val):
    return col.delete_one({filter_key: filter_val})


# for leave swarm
def update_tasks(cols, target_node):
    for col in cols:
        cursor_objs = {}
        for document in col.find({}):
            for task in document['job_info']['tasks']:
                filter_key = 'job_info.tasks.%s.node' % task
                obj = col.find({filter_key: {'$ne': target_node}})
                obj = list(obj)
                if len(obj) != 0:
                    cursor_objs.update({task: obj[0]['job_info']['tasks'][task]})

        col.replace_one({}, {'job_info.tasks': cursor_objs})


def update_workers_resource_col(workers_col, hostname, workers_resource_col):
    target_worker_info = filter_col(workers_col, 'hostname', hostname)
    used_core_num = 0
    total_cores = len(target_worker_info['CPUs'])
    for core in target_worker_info['CPUs']:
        if target_worker_info['CPUs'][core]:
            used_core_num += 1
    used_core_ratio = used_core_num / total_cores
    time_stamp = time.time()
    filter_result = filter_col(workers_resource_col, 'hostname', hostname)
    # initial state
    if filter_result is None:
        resource_info = {
            'hostname': hostname,
            'init_time': time_stamp,
            'details': [[0, 0]]
        }
        insert_doc(workers_resource_col, resource_info)
    else:
        time_stamp = int(time_stamp - filter_result['init_time'])
        filter_result['details'].append([time_stamp, used_core_ratio])
        update_doc(workers_resource_col, 'hostname', hostname, 'details', filter_result['details'])