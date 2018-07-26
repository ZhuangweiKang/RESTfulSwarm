#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import json
import random
from Client.Client import StressClient


class BurstyStressClient(StressClient):
    def __init__(self):
        super(StressClient).__init__()

    def feed_func(self, time_stamp):
        random.expovariate()