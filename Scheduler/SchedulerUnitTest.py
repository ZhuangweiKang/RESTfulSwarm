#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import unittest
import random
import time

import mongodb_api as mg
import utl
from Scheduler.BestFitScheduler import BestFitScheduler
from Scheduler.BestFitDecreasingScheduler import BestFitDecreasingScheduler
from Scheduler.FirstFitScheduler import FirstFitScheduler
from Scheduler.FirstFitDecreasingScheduler import FirstFitDecreasingScheduler


class TestScheduler(unittest.TestCase):

    def setUp(self):
        self.db = mg.get_client(usr='admin',
                                pwd='kzw',
                                address='129.114.108.18',
                                port='27017')
        self.workersInfo = 'WorkersInfo'
        self.workersResourceInfo = 'WorkersResourceInfo'
        self.logger = utl.get_logger('SchedulerUnitTestLogger', 'SchedulerUnitTestLog')

    def bin_packing(self, expect, strategy, request, available):
        self.logger.info('Request: %s' % request)
        self.logger.info('Available: %s' % available)
        scheduling_decision = strategy(request, available)
        self.logger.info('Real result:%s' % scheduling_decision)
        self.logger.info('Expect: %s' % expect)
        return self.assertListEqual(scheduling_decision, expect, msg='Failure: lists are different.')

    def test_bf(self):
        self.logger.info('\n\n')
        self.logger.info('Start testing Best-Fit scheduler...')

        self.logger.info('Test1:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = []
        expect = [(0, -1), (1, -1), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test2:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [4, 5, 3, 7, 4, 2, 1]
        expect = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test3:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [6, 8, 9, 5, 4]
        expect = [(0, 4), (1, 3), (2, 0), (3, 1), (4, 2), (5, 0), (6, 0)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test4:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [3, 3, 3, 3]
        expect = [(0, -1), (1, -1), (2, 0), (3, -1), (4, -1), (5, 1), (6, 1)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test5:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [50]
        expect = [(0, 0), (1, 0), (2, 0), (3, 0), (4, 0), (5, 0), (6, 0)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test6:')
        request = []
        available = [88, 89, 14, 88, 4, 72, 88, 88, 52, 1, 60, 17, 95, 30, 3, 64, 23, 35, 59, 26, 17, 46, 38, 30, 33, 42, 48, 95, 12, 99, 7, 52, 90, 51, 81, 44, 73, 94, 14, 14, 71, 68, 28, 43, 97, 67, 99, 6, 6, 58, 18, 32, 86, 79, 65, 99, 3, 34, 59, 92, 23, 80, 46, 82, 60, 49, 93, 60, 52, 20, 45, 79, 22, 72, 28, 70, 77, 5, 36, 75, 82, 67, 92, 64, 67, 58, 1, 49, 9, 18, 97, 84, 89, 77, 18, 93, 87, 84, 82, 68]
        expect = []
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test7:')
        request = [1]
        available = [48, 18, 12, 44, 20, 25, 28, 13, 23, 38, 11, 39, 22, 20, 1, 10, 48, 27, 48, 10, 37, 39, 11, 13, 33, 11, 17, 45, 25, 37, 9, 49, 48, 7, 9, 40, 44, 42, 33, 38, 16, 45, 23, 18, 12, 24, 29, 36, 31, 14, 25, 46, 26, 11, 7, 23, 11, 11, 3, 6, 39, 42, 45, 34, 30, 35, 48, 20, 7, 3, 1, 26, 49, 26, 13, 15, 7, 15, 11, 44, 7, 22, 13, 35, 14, 22, 43, 11, 13, 27, 9, 10, 16, 43, 36, 32, 31, 32, 2, 19]
        expect = [(0, 14)]
        scheduler = BestFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

    def test_bfd(self):
        self.logger.info('\n\n')
        self.logger.info('Start testing Best-Fit-Decreasing scheduler...')

        self.logger.info('Test1:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [6, 8, 9, 5, 4]
        expect = [(0, 1), (1, 3), (2, 4), (3, 0), (4, 2), (5, 0), (6, 1)]
        scheduler = BestFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test2:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [6, 8, 9]
        expect = [(0, 1), (1, 0), (2, 2), (3, 2), (4, -1), (5, -1), (6, 0)]
        scheduler = BestFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test3:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = []
        expect = [(0, -1), (1, -1), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1)]
        scheduler = BestFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test4:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [9, 8, 7, 6, 5, 4, 3, 2, 1]
        expect = [(0, 2), (1, 4), (2, 5), (3, 3), (4, 6), (5, 3), (6, 8)]
        scheduler = BestFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

        self.logger.info('Test5:')
        request = [48, 18, 12]
        available = [48, 18, 12, 44, 20, 25, 28, 13, 23, 38, 11, 39, 22, 20, 1, 10, 48, 27, 48, 10, 37, 39, 11, 13, 33, 11, 17, 45, 25, 37, 9, 49, 48, 7, 9, 40, 44, 42, 33, 38, 16, 45, 23, 18, 12, 24, 29, 36, 31, 14, 25, 46, 26, 11, 7, 23, 11, 11, 3, 6, 39, 42, 45, 34, 30, 35, 48, 20, 7, 3, 1, 26, 49, 26, 13, 15, 7, 15, 11, 44, 7, 22, 13, 35, 14, 22, 43, 11, 13, 27, 9, 10, 16, 43, 36, 32, 31, 32, 2, 19]
        expect = [(0, 0), (1, 1), (2, 2)]
        scheduler = BestFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.best_fit, request=request, available=available)

    def test_ff(self):
        self.logger.info('\n\n')
        self.logger.info('Start testing First-Fit scheduler...')

        self.logger.info('Test1:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = []
        expect = [(0, -1), (1, -1), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test2:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [4, 5, 3, 7, 4, 2, 1]
        expect = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test3:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [6, 8, 9, 5, 4]
        expect = [(0, 0), (1, 1), (2, 1), (3, 2), (4, 3), (5, 0), (6, 2)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test4:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [3, 3, 3, 3]
        expect = [(0, -1), (1, -1), (2, 0), (3, -1), (4, -1), (5, 1), (6, 1)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test5:')
        request = [4, 5, 3, 7, 4, 2, 1]
        available = [5, 50]
        expect = [(0, 0), (1, 1), (2, 1), (3, 1), (4, 1), (5, 1), (6, 0)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test6:')
        request = []
        available = [88, 89, 14, 88, 4, 72, 88, 88, 52, 1, 60, 17, 95, 30, 3, 64, 23, 35, 59, 26, 17, 46, 38, 30, 33,
                     42, 48, 95, 12, 99, 7, 52, 90, 51, 81, 44, 73, 94, 14, 14, 71, 68, 28, 43, 97, 67, 99, 6, 6, 58,
                     18, 32, 86, 79, 65, 99, 3, 34, 59, 92, 23, 80, 46, 82, 60, 49, 93, 60, 52, 20, 45, 79, 22, 72, 28,
                     70, 77, 5, 36, 75, 82, 67, 92, 64, 67, 58, 1, 49, 9, 18, 97, 84, 89, 77, 18, 93, 87, 84, 82, 68]
        expect = []
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test7:')
        request = [1]
        available = [48, 18, 12, 44, 20, 25, 28, 13, 23, 38, 11, 39, 22, 20, 1, 10, 48, 27, 48, 10, 37, 39, 11, 13, 33,
                     11, 17, 45, 25, 37, 9, 49, 48, 7, 9, 40, 44, 42, 33, 38, 16, 45, 23, 18, 12, 24, 29, 36, 31, 14,
                     25, 46, 26, 11, 7, 23, 11, 11, 3, 6, 39, 42, 45, 34, 30, 35, 48, 20, 7, 3, 1, 26, 49, 26, 13, 15,
                     7, 15, 11, 44, 7, 22, 13, 35, 14, 22, 43, 11, 13, 27, 9, 10, 16, 43, 36, 32, 31, 32, 2, 19]
        expect = [(0, 0)]
        scheduler = FirstFitScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

    def test_ffd(self):
        self.logger.info('\n\n')
        self.logger.info('Start testing First-Fit-Decreasing scheduler...')

        self.logger.info('Test1:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [6, 8, 9, 5, 4]
        expect = [(0, 1), (1, 0), (2, 2), (3, 2), (4, 3), (5, 3), (6, 0)]
        scheduler = FirstFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test2:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [6, 8, 9]
        expect = [(0, 1), (1, 0), (2, 2), (3, 2), (4, -1), (5, -1), (6, 0)]
        scheduler = FirstFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test3:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = []
        expect = [(0, -1), (1, -1), (2, -1), (3, -1), (4, -1), (5, -1), (6, -1)]
        scheduler = FirstFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test4:')
        request = [7, 5, 4, 4, 3, 2, 1]
        available = [9, 8, 7, 6, 5, 4, 3, 2, 1]
        expect = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 1), (5, 0), (6, 2)]
        scheduler = FirstFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test5:')
        request = [48, 18, 12]
        available = [48, 18, 12, 44, 20, 25, 28, 13, 23, 38, 11, 39, 22, 20, 1, 10, 48, 27, 48, 10, 37, 39, 11, 13, 33,
                     11, 17, 45, 25, 37, 9, 49, 48, 7, 9, 40, 44, 42, 33, 38, 16, 45, 23, 18, 12, 24, 29, 36, 31, 14,
                     25, 46, 26, 11, 7, 23, 11, 11, 3, 6, 39, 42, 45, 34, 30, 35, 48, 20, 7, 3, 1, 26, 49, 26, 13, 15,
                     7, 15, 11, 44, 7, 22, 13, 35, 14, 22, 43, 11, 13, 27, 9, 10, 16, 43, 36, 32, 31, 32, 2, 19]
        expect = [(0, 0), (1, 1), (2, 2)]
        scheduler = FirstFitDecreasingScheduler(self.db)
        self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        self.logger.info('Test6:')
        request = []
        available = []
        for i in range(10000):
            request.append(random.randint(1, 50))
            available.append(random.randint(50, 100))

        start = time.time()
        # expect = [(0, 0), (1, 1), (2, 2)]
        scheduler = FirstFitDecreasingScheduler(self.db)

        # self.bin_packing(expect=expect, strategy=scheduler.first_fit, request=request, available=available)

        print(scheduler.first_fit(request, available))
        print('Elapsed time:', 1000 * (time.time() - start))