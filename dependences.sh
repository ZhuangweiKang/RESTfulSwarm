#!/bin/bash

install_python_libs(){
    # install pip3
    apt-get install -y python3-pip python3-dev python3-setuptools xmlto asciidoc

    # install python packages for docker , flask, pyzmq, mongodb, cpuinfo, pandas
    pip3 install flask docker pyzmq pymongo py-cpuinfo flasgger pandas requests
}


install_docker(){
    # check if docker exists
    docker_check=$(which docker)

    if [ "$docker_check" = "" ];
    then
        # install docker 17.12
        wget https://download.docker.com/linux/ubuntu/dists/xenial/pool/stable/amd64/docker-ce_17.12.0~ce-0~ubuntu_amd64.deb
        dpkg -i docker-ce_17.12.0~ce-0~ubuntu_amd64.deb

        # run docker without sudo
        groupadd docker
        usermod -aG docker \$USER
    fi

    # check docker experimental feature
    docker_version=$(docker version | grep Experimental)
    docker_version=${docker_version:15}

    if [ "$docker_version" = "false" ];
    then
        echo "{\"experimental\": true}" > /etc/docker/daemon.json
        # restart docker daemon
        systemctl restart docker
        systemctl daemon-reload
    fi

    echo $(docker version)
}


install_criu(){
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
}


install_db(){
    # install mongodb 3.2 on ubuntu 16.04
    apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv EA312927
    echo "deb http://repo.mongodb.org/apt/ubuntu xenial/mongodb-org/3.2 multiverse" | sudo tee /etc/apt/sources.list.d/mongodb-org-3.2.list
    apt-get update
    apt-get install -y mongodb-org
    systemctl start mongod
    systemctl status mongod
    systemctl enable mongod

    # create user and db
    printf "use $3\ndb.createUser( { user: \"$1\", pwd: \"$2\", roles: [ { role: \"readWrite\", db: \"$3\" } ] } )" | mongo

    systemctl restart mongod

    # reconfigure mongodb to allow remote access
    mv ./mongod.conf /etc/mongod.conf
    systemctl restart mongod
}


install_weave(){
    # install weave plugin and enable multicast feature
    docker plugin install weaveworks/net-plugin:latest_release
    docker plugin disable weaveworks/net-plugin:latest_release
    docker plugin set weaveworks/net-plugin:latest_release WEAVE_MULTICAST=1
    docker plugin enable weaveworks/net-plugin:latest_release
}


install_nfs_master(){
    # configure nfs
    apt-get update
    apt-get install nfs-kernel-server
    mkdir /var/nfs/RESTfulSwarm/ -p
    chown nobody:nogroup /var/nfs/RESTfulSwarm
}


install_nfs_client(){
    # configure NFS
    apt-get update
    apt-get install nfs-common
    mkdir -p /nfs/RESTfulSwarm
}

main(){
    apt-get update && install libltdl7
    if [ "$1" = "DB" ]
    then
        install_db $2 $3 $4
        return 1
    fi
    install_python_libs
    if [ "$1" = "GM" ]
    then
        install_docker
        install_criu
        install_weave
        install_nfs_master
    elif [ "$1" = "Worker" ]
    then
        install_docker
        install_criu
        install_weave
        install_nfs_client
    fi
    return 1
}

main $1 $2 $3 $4