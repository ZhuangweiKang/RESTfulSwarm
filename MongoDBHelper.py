#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import pymongo as mg


# return a mongodb client
def get_client(address, port=27017):
    url = 'mongodb://admin:kzw@%s:%s/RESTfulSwarmDB' % (address, port)
    return mg.MongoClient(url)


# return a database object
def get_db(client, db_name):
    return client[db_name]


# return a collection cursor object
def get_col(db, col_name):
    return db[col_name]


def find_col(col):
    return list(col.find({}))


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
    return col.update_one({filter_key: filter_value}, {"$set": {target_key: target_value}}, upsert=True)


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