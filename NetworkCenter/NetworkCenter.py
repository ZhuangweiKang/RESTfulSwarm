#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import argparse
from flask import *
import paramiko as pk
from flasgger import Swagger, swag_from

app = Flask(__name__)

template = {
  "swagger": "2.0",
  "info": {
    "title": "RESTfulSwarm Network Center",
    "description": "The network center for RESTfulSwarm application.",
    "contact": {
      "responsibleDeveloper": "Zhuangwei Kang",
      "email": "zhuangwei.kang@vanderbilt.edu"
    },
    "version": "0.0.1"
  },
  "host": "129.114.109.219:5000",
  "basePath": "",  # base bash for blueprint registration
  "schemes": [
    "http",
  ]
}

swagger = Swagger(app, template=template)


def ssh_exec_cmd(address, usr, pkey, cmd):
    key = pk.RSAKey.from_private_key_file(pkey)
    con = pk.SSHClient()
    con.set_missing_host_key_policy(pk.AutoAddPolicy)
    con.connect(hostname=address, username=usr, pkey=key)
    stdin, stdout, stderr = con.exec_command(cmd)
    print('Executed command %s on host %s' % (cmd, address))
    print('Execution result:', stdout.read(), stderr.read())
    con.close()


@app.route('/RESTfulSwarm/NC/nc_client', methods=['POST'])
@swag_from('nc_client.yml', validation=True)
def nc_client():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    dport = data['dst_port']
    delay = data['delay']
    variation = data['variation']
    distribution = data['distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:1' \
          % (nw_device, nw_device, nw_device, delay, variation, distribution, nw_device, dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_fe', methods=['POST'])
@swag_from('nc_fe.yml', validation=True)
def nc_fe():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    jm_dport = data['jm_dst_port']
    jm_delay = data['jm_delay']
    jm_variation = data['jm_variation']
    jm_distribution = data['jm_distribution']

    db_dport = data['db_dport']
    db_delay = data['db_delay']
    db_variation = data['db_variation']
    db_distribution = data['db_distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc class add dev %s parent 1: classid 1:2 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc qdisc add dev %s parent 1:2 handle 20: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 2 u32 match tcp dport %s flowid 1:1 \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:2 \ ' \
          % (nw_device, nw_device, nw_device, nw_device, jm_delay, jm_variation, jm_distribution,
             nw_device, db_delay, db_variation, db_distribution, nw_device, jm_dport, nw_device, db_dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_jm', methods=['POST'])
@swag_from('nc_jm.yml', validation=True)
def nc_jm():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    gm_dport = data['gm_dst_port']
    gm_delay = data['gm_delay']
    gm_variation = data['gm_variation']
    gm_distribution = data['gm_distribution']

    db_dport = data['db_dport']
    db_delay = data['db_delay']
    db_variation = data['db_variation']
    db_distribution = data['db_distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc class add dev %s parent 1: classid 1:2 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc qdisc add dev %s parent 1:2 handle 20: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 2 u32 match tcp dport %s flowid 1:1 \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:2 \ ' \
          % (nw_device, nw_device, nw_device, nw_device, gm_delay, gm_variation, gm_distribution,
             nw_device, db_delay, db_variation, db_distribution, nw_device, gm_dport, nw_device, db_dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_gm', methods=['POST'])
@swag_from('nc_gm.yml', validation=True)
def nc_gm():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    worker_sport = data['worker_sport']
    worker_delay = data['worker_delay']
    worker_variation = data['worker_variation']
    worker_distribution = data['worker_distribution']

    db_dport = data['db_dport']
    db_delay = data['db_delay']
    db_variation = data['db_variation']
    db_distribution = data['db_distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc class add dev %s parent 1: classid 1:2 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc qdisc add dev %s parent 1:2 handle 20: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol ip parent 1:0 prio 2 u32 match ip sport %s flowid 1:1 \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:2' \
          % (nw_device, nw_device, nw_device, nw_device, worker_delay, worker_variation, worker_distribution,
             nw_device, db_delay, db_variation, db_distribution, nw_device, worker_sport, nw_device, db_dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_worker', methods=['POST'])
@swag_from('nc_worker.yml', validation=True)
def nc_worker():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    worker_sport = data['worker_sport']
    worker_delay = data['worker_delay']
    worker_variation = data['worker_variation']
    worker_distribution = data['worker_distribution']

    dis_dport = data['dis_dport']
    dis_delay = data['dis_delay']
    dis_variation = data['dis_variation']
    dis_distribution = data['dis_distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc class add dev %s parent 1: classid 1:2 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc qdisc add dev %s parent 1:2 handle 20: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol ip parent 1:0 prio 2 u32 match ip sport %s flowid 1:1 \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:2 \ ' \
          % (nw_device, nw_device, nw_device, nw_device, worker_delay, worker_variation, worker_distribution,
             nw_device, dis_delay, dis_variation, dis_distribution, nw_device, worker_sport, nw_device, dis_dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_discovery', methods=['POST'])
@swag_from('nc_discovery.yml', validation=True)
def nc_discovery():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    dport = data['dst_port']
    delay = data['delay']
    variation = data['variation']
    distribution = data['distribution']
    cmd = 'tc qdisc add dev %s handle 1: root htb \ ' \
          'tc class add dev %s parent 1: classid 1:1 htb \ ' \
          'tc qdisc add dev %s parent 1:1 handle 10: netem delay %dms %dms distribution %s \ ' \
          'tc filter add dev %s protocol tcp parent 1:0 prio 1 u32 match tcp dport %s flowid 1:1' \
          % (nw_device, nw_device, nw_device, delay, variation, distribution, nw_device, dport)
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


@app.route('/RESTfulSwarm/NC/nc_del', methods=['POST'])
@swag_from('nc_del.yml', validation=True)
def nc_del():
    data = request.get_json()
    address = data['address']
    usr = data['usr']
    pkey = data['pkey']
    nw_device = data['network_device']
    cmd = 'tc qdisc del dev %s handle 1: root htb' % nw_device
    ssh_exec_cmd(address=address, usr=usr, pkey=pkey, cmd=cmd)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('-a', '--address', type=str, help='Network center IP address')
    parser.add_argument('-p', '--port', type=str, help='Network center port number')
    args = parser.parse_args()
    address = args.address
    port = args.port
    app.run(host=address, port=port, debug=True)