#!/bin/bash

# install docker sdk
pip install docker

# check if docker exists
docker_check=$(which docker)

if [ "$docker_check" = "" ];
then
    # install docker
    cd /home/ubuntu
    wget https://download.docker.com/linux/ubuntu/dists/xenial/pool/stable/amd64/docker-ce_17.03.0~ce-0~ubuntu-xenial_amd64.deb
    dpkg -i docker-ce_17.03.0~ce-0~ubuntu-xenial_amd64.deb

    # run docker without sudo
    groupadd docker
    usermod -aG docker $USER
fi


# check docker experimental feature
docker_version=$(docker version | grep Experimental)

if [ "$docker_version" = "$false" ];
then
    echo "{\"experimental\": true}" >> /etc/docker/daemon.json
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

