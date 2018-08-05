#!/bin/bash

apt-get update

apt-get install libltdl7

# install pip3
apt-get install -y python3-pip python3-dev python3-setuptools xmlto asciidoc

# install python packages for docker , flask, pyzmq, mongodb, cpuinfo, pandas
pip3 install flask docker pyzmq pymongo py-cpuinfo flasgger pandas


# check if docker exists
docker_check=$(which docker)

if [ "$docker_check" = "" ];
then
    # install docker 17.12
    wget https://download.docker.com/linux/ubuntu/dists/xenial/pool/stable/amd64/docker-ce_17.12.0~ce-0~ubuntu_amd64.deb
    dpkg -i docker-ce_17.12.0~ce-0~ubuntu_amd64.deb

    # run docker without sudo
    groupadd docker
    usermod -aG docker $USER
fi


# check docker experimental feature
docker_version=$(docker version | grep Experimental)
docker_version=${docker_version:15}

if [ $docker_version = "false" ];
then
    echo "{\"experimental\": true}" > /etc/docker/daemon.json
    # restart docker daemon
    systemctl restart docker
    systemctl daemon-reload
fi

echo $(docker version)

# check if CRIU exists
criu_check=$(which criu)
if [ "$criu_check" = "" ];
then
    apt-get update && sudo apt-get install -y protobuf-c-compiler libprotobuf-c0-dev protobuf-compiler libprotobuf-dev:amd64 gcc build-essential bsdmainutils python git-core asciidoc make htop git curl supervisor cgroup-lite libapparmor-dev libseccomp-dev libprotobuf-dev libprotobuf-c0-dev protobuf-c-compiler protobuf-compiler python-protobuf libnl-3-dev libcap-dev libaio-dev apparmor libnet-dev
    cd /home/ubuntu
    git clone https://github.com/xemul/criu criu
    cd criu
    make clean
    make
    make install

    # check if criu works well
    echo $(criu check)
    echo $(criu check --all)
fi

# install mongodb 3.2 on ubuntu 16.04
apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
apt-get update
apt-get install -y mongodb-org
systemctl start mongod
systemctl status mongod

# install weave plugin and enable multicast feature
docker plugin install weaveworks/net-plugin:latest_release
docker plugin disable weaveworks/net-plugin:latest_release
docker plugin set weaveworks/net-plugin:latest_release WEAVE_MULTICAST=1
docker plugin enable weaveworks/net-plugin:latest_release