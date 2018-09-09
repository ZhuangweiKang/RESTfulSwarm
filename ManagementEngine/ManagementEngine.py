#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os
import sys
import json
import paramiko as pk
import time
import multiprocessing

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from FrontEnd import FrontEnd as FE
from JobManager import JobManager as JM
from GlobalManager import GlobalManager as GM
from Discovery import Discovery as DC

from Client import BurstyStressClient as BC
from Client import IncrementalStressClient as IC
from Client import RandomStressClient as RC
from Client import SteadyStressClient as SC

import mongodb_api as mg
import docker_api as docker
import SystemConstants


class ManagementEngine(object):
    def __init__(self, db_info, workers_details):
        self.__db_client = mg.get_client(usr=db_info['user'], pwd=db_info['pwd'], db_name=db_info['db_name'],
                                         address=db_info['address'], port=SystemConstants.MONGODB_PORT)
        self.__db = mg.get_db(self.__db_client, db_name=SystemConstants.MONGODB_NAME)
        self.__workers_info = workers_details

    def reset_db(self):
        all_cols = mg.get_all_cols(self.__db)
        if SystemConstants.WorkersResourceInfo in all_cols:
            # Drop worker resource info collection
            mg.drop_col(self.__db_client, SystemConstants.MONGODB_NAME, SystemConstants.WorkersResourceInfo)

        if SystemConstants.WorkersInfo in all_cols:
            # Reset worker info collection
            workers_info_col = mg.get_col(self.__db, SystemConstants.WorkersInfo)
            workers_info_data = mg.find_col(workers_info_col)
            for index, worker in enumerate(workers_info_data[:]):
                for cpu in worker['CPUs']:
                    workers_info_data[index]['CPUs'][cpu] = False
                mg.update_doc(col=workers_info_col,
                              filter_key='hostname',
                              filter_value=worker['hostname'],
                              target_key='CPUs',
                              target_value=workers_info_data[index]['CPUs'])
        print('Reset MongoDB.')

    @staticmethod
    def clear_master():
        # let master node leave swarm
        docker.leave_swarm(docker.set_client())
        print('Master node left swarm.')

    @staticmethod
    def launch_fe():
        fe_process = multiprocessing.Process(
            name='FrontEnd',
            target=FE.main
        )
        fe_process.daemon = True
        fe_process.start()
        print('Launched FrontEnd.')
        return fe_process

    @staticmethod
    def launch_jm():
        jm_process = multiprocessing.Process(
            name='JobManager',
            target=JM.JobManager.main
        )
        jm_process.daemon = True
        jm_process.start()
        print('Launched JobManager.')
        return jm_process

    @staticmethod
    def launch_gm():
        gm_process = multiprocessing.Process(
            name='GlobalManager',
            target=GM.main
        )
        gm_process.daemon = True
        gm_process.start()
        print('Launched GlobalManager.')
        return gm_process

    @staticmethod
    def launch_discovery():
        dc_process = multiprocessing.Process(
            name='Discovery',
            target=DC.Discovery.main
        )
        dc_process.daemon = True
        dc_process.start()
        print('Launched Discovery.')
        return dc_process

    @staticmethod
    def ssh_exec_cmd(address, usr, cmd):
        key = pk.RSAKey.from_private_key_file(SystemConstants.PRIVATE_KEY)
        con = pk.SSHClient()
        con.set_missing_host_key_policy(pk.AutoAddPolicy)
        con.connect(hostname=address, username=usr, pkey=key)
        con.exec_command(cmd)
        print('Executed command %s on worker %s' % (cmd, address))
        con.close()

    def launch_workers(self):
        for worker in self.__workers_info:
            self.ssh_exec_cmd(address=worker['address'], usr=worker['user'], cmd=worker['launch_worker'])
            time.sleep(1)
        print('Waiting for workers joining.')
        while len(docker.get_node_list(docker.set_client())) == 0:
            pass
        print('Launched all workers.')

    @staticmethod
    def launch_client(session_id):
        client_process = multiprocessing.Process(
            name='Client',
            target=IC.IncrementalStressClient.main,
            args=(session_id, )
        )
        client_process.daemon = True
        client_process.start()
        print('Launched Client.')
        return client_process

    @staticmethod
    def shutdown_process(process):
        process.terminate()

    def shutdown_workers(self):
        for worker in self.__workers_info:
            self.ssh_exec_cmd(worker['address'], worker['user'], worker['kill_worker'])
        print('Shutdown all workers.')

    def launch_system(self):
        print('Start launching system.')
        self.clear_master()
        time.sleep(1)
        # self.shutdown_workers()
        time.sleep(1)
        self.reset_db()
        time.sleep(1)
        fe_process = self.launch_fe()
        time.sleep(1)
        gm_process = self.launch_gm()
        time.sleep(1)
        jm_process = self.launch_jm()
        time.sleep(1)
        dc_process = self.launch_discovery()
        time.sleep(1)
        # self.launch_workers()
        time.sleep(1)
        session_id = str(int(time.time()))
        client_process = ""# self.launch_client(session_id=session_id)
        return fe_process, jm_process, gm_process, dc_process, client_process

    def shutdown_system(self, fe_process, jm_process, gm_process, dc_process, client_process):
        print('Start shutting down system.')
        # self.reset_db()
        print('Reset MongoDB.')
        # self.shutdown_process(client_process)
        print('Reset client.')
        self.shutdown_process(fe_process)
        print('Reset FrontEnd.')
        self.shutdown_process(jm_process)
        print('Reset JobManager.')
        self.shutdown_process(gm_process)
        print('Reset GlobalManager.')
        self.shutdown_process(dc_process)
        print('Reset Discovery.')
        # self.shutdown_workers()
        print('Reset Workers.')
        self.clear_master()
        print('Reset Master.')
        time.sleep(5)
       # self.reset_db()

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
    with open('WorkersInfo.json') as f:
        workers_info = json.load(f)

    with open('../DBInfo.json') as f:
        db_info = json.load(f)

    me = ManagementEngine(db_info=db_info, workers_details=workers_info)
    me.main()
