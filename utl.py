#!/usr/bin/env /usr/local/bin/python
# encoding: utf-8
# Author: Zhuangwei Kang

import os, sys
import socket
import struct
import tarfile
import shutil
import logging


def doLog(loggerName, logFile):
    logger = logging.getLogger(loggerName)
    logger.setLevel(logging.DEBUG)

    fl = logging.FileHandler(logFile)
    fl.setLevel(logging.DEBUG)

    cl = logging.StreamHandler()
    cl.setLevel(logging.DEBUG)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fl.setFormatter(formatter)
    cl.setFormatter(formatter)

    logger.addHandler(fl)
    logger.addHandler(cl)

    return logger


def getHostName():
    cmd = 'hostname'
    return os.popen(cmd).read().strip()


def getHostIP():
    host_addr = os.popen('ip addr show dev eno1 | grep inet | awk \'{print $2}\' | head -n 1', 'r').read()
    host_addr = host_addr.split('/')[0]
    return host_addr


def getWorkDir():
    return '/var/lib/docker/tmp'


def goToWorkDir():
    workDir = '/var/lib/docker/tmp'
    os.chdir(workDir)


def tarFiles(checkpointTar, containerID, checkpointName):
    checkpointDir = '/var/lib/docker/containers/%s/checkpoints' % containerID
    os.chdir(checkpointDir)
    tar_file = tarfile.TarFile.open(name=checkpointTar, mode='w')
    checkpointTarFile = checkpointDir + '/' + checkpointName
    tar_file.add(checkpointTarFile, arcname=os.path.basename(checkpointTarFile))
    tar_file.close()
    shutil.move(checkpointTar, getWorkDir())
    goToWorkDir()


def untarFile(tarFile):
    goToWorkDir()
    tar = tarfile.TarFile.open(name=tarFile, mode='r')
    tar.extractall()
    tar.close()
    os.remove(tarFile)


def transferFile(fileName, dst_addr, port, logger):
    try:
        logger.info('Prepare to send tar file to destination host.')
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((dst_addr, port))
        logger.info('Connection has been set up.')
    except socket.error as er:
        logger.error(er)
        sys.exit(1)
    if os.path.isfile(fileName):
        fileinfo_size = struct.calcsize('128sl')
        fhead = struct.pack('128sl', os.path.basename(fileName), os.stat(fileName).st_size)
        s.send(fhead)

        fp = open(fileName, 'rb')
        while True:
            data = fp.read(1024)
            if not data:
                break
            s.sendall(data)
        logger.info('Tar file has been sent.')
        fp.close()
        s.close()
    else:
        logger.error('File %s not exists.' % fileName)
        sys.exit(1)


def recvFile(logger, port=3300):
    try:
        recvSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        recvSocket.bind(('', port))
        recvSocket.listen(20)
        logger.info('Waiting for client to connect...')
        conn, addr = recvSocket.accept()
        logger.info('Client has connected to server...')
        goToWorkDir()
        fileinfo_size = struct.calcsize('128sl')
        fhead = conn.recv(fileinfo_size)
        fn, fileSize = struct.unpack('128sl', fhead)
        logger.info('Received file info: %s' % fn)
        logger.info('File size: ' + str(fileSize))
        fileName = fn.strip('\00')
        with open(fileName, 'wb') as tarFile:
            logger.info('Start receiving file...')
            tempSize = fileSize
            while True:
                if tempSize > 1024:
                    data = conn.recv(1024)
                else:
                    data = conn.recv(tempSize)
                if not data:
                    break
                tarFile.write(data)
                tempSize -= len(data)
                if tempSize == 0:
                    break
            logger.info('Receiving file finished, connection will be closed...')
        conn.close()
        recvSocket.close()
        logger.info('Connection has been closed...')
        return fn
    except Exception as ex:
        logger.error(ex)
        return None