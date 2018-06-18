#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import pymongo as mg


# return a mongodb client
def get_client(address, port=27017):
    return mg.MongoClient(address, port)


# return a database object
def get_db(client, db_name):
    return client[db_name]


# return a collection object
def get_col(db, col_name):
    return db[col_name]


# delete a collection(a job in my work)
def drop_col(client, db_name, col_name):
    db = client[db_name]
    col = db[col_name]
    col.drop()


def insert_doc(col, data):
    return col.insert_one(data).inserted_id


def update_doc(col, filter_key, filter_value, target_key, target_value):
    return col.update_one({filter_key: filter_value}, {"$set": {target_key: target_value}}, upsert=False)