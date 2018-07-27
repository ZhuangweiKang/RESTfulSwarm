#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from FrontEnd import FrontEnd as fe
from JobManager import JobManager as jm
from GlobalManager import GlobalManager as gm
from Discovery import Discovery as dc

from Client import BurstyStressClient as bursty_client
from Client import IncrementalStressClient as incremental_client
from Client import RandomStressClient as random_client
from Client import SteadyStressClient as steay_client

import multiprocessing
import paramiko as pk
import time
import MongoDBHelper as mg
import DockerHelper as dh
import json


class ManagementEngine:
    def __init__(self, db_packet, workers_info, private_key_file):
        db_addr = db_packet['address']
        db_pwd = db_packet['pwd']
        db_port = db_packet['port']
        db_usr = db_packet['user']
        self.db_name = db_packet['database']
        self.mg_client = mg.get_client(usr=db_usr, pwd=db_pwd, address=db_addr, port=db_port)
        self.db = mg.get_db(self.mg_client, db_name=self.db_name)
        self.workers_info = workers_info
        self.private_key_file = private_key_file

    def clear_db(self):
        cols = mg.get_all_cols(self.db)
        for col in cols:
            mg.drop_col(self.mg_client, self.db_name, col)
        print('Cleaned MongoDB.')

    def clear_master(self):
        # let master node leave swarm
        dh.leaveSwarm(dh.setClient())
        print('Master node left swarm.')

    def launch_fe(self):
        fe_pro = multiprocessing.Process(
            name='FrontEnd',
            target=fe.main
        )
        fe_pro.daemon = True
        fe_pro.start()
        print('Launched FrontEnd.')
        return fe_pro

    def launch_jm(self):
        jm_pro = multiprocessing.Process(
            name='JobManager',
            target=jm.main
        )
        jm_pro.daemon = True
        jm_pro.start()
        print('Launched JobManager.')
        return jm_pro

    def launch_gm(self):
        gm_pro = multiprocessing.Process(
            name='GlobalManager',
            target=gm.main
        )
        gm_pro.daemon = True
        gm_pro.start()
        print('Launched GlobalManager.')
        return gm_pro

    def launch_discovery(self):
        dc_pro = multiprocessing.Process(
            name='Discovery',
            target=dc.main
        )
        dc_pro.daemon = True
        dc_pro.start()
        print('Launched Discovery.')
        return dc_pro

    def ssh_exec_cmd(self, addr, usr, cmd):
        key = pk.RSAKey.from_private_key_file(self.private_key_file)
        con = pk.SSHClient()
        con.set_missing_host_key_policy(pk.AutoAddPolicy)
        con.connect(hostname=addr, username=usr, pkey=key)
        con.exec_command(cmd)
        print('Executed command %s on worker %s' % (cmd, addr))

    def launch_workers(self):
        for worker in self.workers_info:
            self.ssh_exec_cmd(worker['address'], worker['user'], worker['launch_worker'])
        print('Launched all workers.')

    def launch_client(self):
        client_proc = multiprocessing.Process(
            name='Client',
            target=bursty_client.main
        )
        client_proc.daemon = True
        client_proc.start()
        print('Launched Client.')
        return client_proc

    def shutdown_fe(self, fe_pro):
        fe_pro.terminate()
        print('Shutdown FrontEnd.')

    def shutdown_jm(self, jm_pro):
        jm_pro.terminate()
        print('Shutdown JobManager.')

    def shutdown_gm(self, gm_pro):
        gm_pro.terminate()
        print('Shutdown GlobalManager.')

    def shutdown_discovery(self, dc_pro):
        dc_pro.terminate()
        print('Shutdown Discovery.')

    def shutdown_workers(self):
        for worker in self.workers_info:
            self.ssh_exec_cmd(worker['address'], worker['user'], worker['kill_worker'])
        print('Shutdown all workers.')

    def shutdown_client(self, client_pro):
        client_pro.terminate()
        print('Shutdown Client.')

    def launch_system(self):
        print('Start launching system.')
        self.clear_master()
        time.sleep(1)
        self.clear_db()
        time.sleep(1)
        fe_proc = self.launch_fe()
        time.sleep(1)
        gm_proc = self.launch_gm()
        time.sleep(1)
        jm_proc = self.launch_jm()
        time.sleep(1)
        dc_proc = self.launch_discovery()
        time.sleep(1)
        self.launch_workers()
        time.sleep(1)
        client_proc = self.launch_client()
        time.sleep(1)
        return fe_proc, jm_proc, gm_proc, dc_proc, client_proc

    def shutdown_system(self, fe_proc, jm_proc, gm_proc, dc_proc, client_proc):
        print('Start shutting down system.')
        self.shutdown_client(client_proc)
        time.sleep(1)
        self.shutdown_fe(fe_proc)
        time.sleep(1)
        self.shutdown_jm(jm_proc)
        time.sleep(1)
        self.clear_master()
        time.sleep(1)
        self.shutdown_gm(gm_proc)
        time.sleep(1)
        self.shutdown_workers()
        time.sleep(1)
        self.shutdown_discovery(dc_proc)
        time.sleep(1)
        self.clear_db()

    def main(self):
        while True:
            switch_on = input('Would you like to launch RESTfulSwarm system?(y/n) ')
            if switch_on == 'y':
                processes = self.launch_system()
                while True:
                    switch_off = input('Would you like to shutdown RESTfulSwarm system?(y/n) ')
                    if switch_off == 'y':
                        self.shutdown_system(processes[0], processes[1], processes[2], processes[3], processes[4])
                        break


if __name__ == '__main__':
    with open('DBinfo.json') as f:
        db_packet = json.load(f)

    with open('WorkersInfo.json') as f:
        workers_info = json.load(f)

    private_key_file = 'private_key.pem'
    me = ManagementEngine(db_packet=db_packet, workers_info=workers_info, private_key_file=private_key_file)
    me.main()