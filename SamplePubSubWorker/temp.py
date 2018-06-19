#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import pymongo as mg
import pprint


def get_client(address, port=27017):
    return mg.MongoClient(address, port)


def get_db(client, db_name):
    return client[db_name]


def get_col(db, col_name):
    return db[col_name]


def drop_col(client, col):
    return col.drop()


def insert_doc(col, data):
    return col.insert_one(data).inserted_id


def update_doc(col, id, key, value):
    return col.update_one({"_id": id}, {"$set": {key: value}}, upsert=False)


def get_col_id(col, key, value):
    return col.find_one({key: value}).get('_id')


def delete_doc(col, key, value):
    return col.delete_one({key: value})


if __name__ == '__main__':
    client = get_client('localhost')
    db = get_db(client, 'MyTest')
    col = get_col(db, 'kang')

    # drop old collection
    drop_col(client, col)

    data = {
        "job_name": "kang",
        "job_info": {
            "network": {
                "name": "kangNetwork",
                "driver": "overlay",
                "subnet": "10.52.0.114/24"
            },
            "tasks": {
                "Publisher": {
                    "node": "kang3",
                    "container_name": "Publisher",
                    "image": "zhuangweikang/publisher",
                    "detach": True,
                    "command": "",
                    "cpuset_cpus": "0,1",
                    "mem_limit": "10m",
                    "ports": {},
                    "volumes": {}
                },
                "Subscriber": {
                    "node": "kang2",
                    "container_name": "Subscriber",
                    "image": "zhuangweikang/subscriber",
                    "detach": True,
                    "command": "python SubscribeData.py -a 10.52.0.2 -p 3000",
                    "cpuset_cpus": "0,1",
                    "mem_limit": "10m",
                    "ports": {},
                    "volumes": {}
                },
                "Subscriber2": {
                    "node": "kang2",
                    "container_name": "Subscriber2",
                    "image": "zhuangweikang/subscriber",
                    "detach": True,
                    "command": "python SubscribeData.py -a 10.52.0.2 -p 3000",
                    "cpuset_cpus": "0,1",
                    "mem_limit": "10m",
                    "ports": {},
                    "volumes": {}
                }
            }
        },
        "status": "Ready"
    }

    insert_id = insert_doc(col, data)

    print(insert_id)

    print('-------------------------------------')

    # display all documentations
    all_info = col.find({})

    print(all_info)
    print('-------------------------------------')

    # display all containers that are in node kang2
    kang2Cons = col.find({
        "job_info": {
            "tasks": {
                "node": "kang2"
            }
        }
    })
    print(kang2Cons)
    print('-------------------------------------')

    # update node name for container
    container = 'Publisher'
    filter_key = 'job_info.tasks.%s.container_name' % container
    key = 'job_info.tasks.%s.node' % container
    value = 'kang5'
    col.update_one({filter_key: 'Publisher'}, {"$set": {key: value}})
    print(col.find({}))

    # update job status
    col.update_one({'status': 'Ready'}, {'$set': {'status': 'Deployed'}})

    # delete job
    '''
    get_input = input('delete job?')
    if get_input == 'y':
    	col.delete_one({'job_info.job_name': 'kang'})
    '''

    # get all collections localted on a specified node via iterating documents
    cursor_objs = []
    for document in col.find({}):
        for task in document['job_info']['tasks']:
            filter_key = 'job_info.tasks.%s.node' % task
            print(filter_key)
            obj = col.find({filter_key: {'$ne': 'kang2'}})
            cursor_objs.append(obj)

    for item in cursor_objs:
        print(list(item))