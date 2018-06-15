#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import pymongo as mg


def get_client(address, port=27017):
    return mg.MongoClient(address, port)


def get_db(client, db_name):
    return client[db_name]


def get_col(db, col_name):
    return db[col_name]


def drop_col(client, db_name, col_name):
    db = client[db_name]
    col = db[col_name]
    col.drop()


def insert_doc(col, data):
    return col.insert_one(data)
