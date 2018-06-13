RESTfulSwarm
============
An API for some basic container operations in Swarm environment.
## [Environment(Prerequisites)](https://github.com/doc-vu/RESTfulSwarm/blob/master/dependences.sh)
* Ubuntu 16.04
* Python3
* Docker 17.03 (with experimental feature)
* CRIU
* Flask
* Pyzmq
## Architecture
* Layer1: [Client](https://github.com/doc-vu/RESTfulSwarm/blob/master/Client/Client.py)
* Available commands now；
>>> 1. Init Swarm Environment (with overlay network name and subnet)
>>> 2. Create Container (using Json file)
>>> 3. Migrate Container
>>> 4. Update Container Resources (including cpuset_cpus and mem_limits)
>>> 5. Leave Swarm Environment (Not only turn down worker node but also completely remove it from swarm manager node.)
>>> 6. Describe Worker Node
>>> 7. Describe Manager Node
>>> 8. Exit
* Layer2: [Global Manager(Swarm Manager Node)](https://github.com/doc-vu/RESTfulSwarm/blob/master/GlobalManager/GlobalManager.py)
>> Global Manager is responsible for creating Swarm environment, building overlay network and sending commands to worker nodes.
* Layer3: [Worker Node](https://github.com/doc-vu/RESTfulSwarm/blob/master/Worker/Worker.py) <br/>
### Steps of playing with the API (Using the PubSub example in SamplePubSubWorker directory): 
* Step1: Start Global Manager
```Bash
python3 GlobalManager.py
```
* Step2: Start Client
```Bash
python3 Client.py -a your_global_manager_addr -p 5000
```
* Step3: Issue "Init" command on Client node
```Bash
# Choose option "1. Init Swarm" in client menu
# Input your overlay network name (make sure it matches the network name in your container Json file)
# Input your Subnet CIDR: for example 10.52.0.114/24
```
* Step4: Make worker nodes join the Swarm environment
```Bash
# On Worker Node:
python3 Worker.py -ma your_global_manager_addr -sa worker_addr
```
* Step5: Create container
```Bash
# Choose option "2. Create Container" on Client node
# Enter the path for your container defination Json file
```
* Optional steps:
```Bash
# Update container resources:
# Choose option4 "Update Container"
# Enter your new cpuset_cpus(for example: 1, 2)
# Enter your new mem_limits(for example: 20m)
# ---------------------------------
# Migrate Container:
# Choose option3 "Migrate Container"
# Enter the container name you want to migrate
# Enter the source host addr
# Enter the destination host addr
# ----------------------------------
# Leave Swarm:
# Choose option5
# Enter your container name
# ----------------------------------
# Describe Worker Node:
# Choose option6:
# Enter your worker node host name
# ----------------------------------
# Describe Manager Node:
# Choose option7:
# Enter your manager node host name
```
` Note: If you only want to call those functionality methods, just import Client.py.`
