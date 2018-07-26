#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from kazoo.client import KazooClient
from kazoo.client import KazooState


class ManagementEngine:
    def __init__(self):
        pass