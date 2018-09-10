# RESTfulSwarm

An environment for core simulation based on Docker Swarm

![Swarm](http://blog.pridybailo.com/wp-content/uploads/sites/2/2015/01/swar-q.png)

## [Environment (Prerequisites)](https://github.com/doc-vu/RESTfulSwarm/blob/master/dependences.sh)

* Ubuntu 16.04
* Python 3
* Docker >=17.12 (with experimental feature)
* CRIU
* Flask
* ZeroMQ
* Swagger
* MongoDB
* NFS

```Bash
# configure SC/FE/JM
./dependences.sh

# configure GM
./dependences.sh GM

# configure Worker
./dependences.sh Worker

# configure DB
# example: ./dependences.sh DB myusr mypwd mydb
./dependences.sh DB $usr $pwd $db $address
# add user and reconfigure mongodb to allow remote access
mongo < initdb.js
mv ./mongod.conf /etc/mongod.conf
systemctl restart mongod
systemctl status mongod
```

## Notion

* Actor：Main components in the skeleton
  1. FE: FrontEnd
  2. JM: JobManager
  3. GM: GlobalManager
  4. DC: Discovery

* Job: A list of containers with same network 
  1. Job name format: 'job'Submit_time-session_id
     >> example: job1533585500968-1533585495

* Task: Container in Docker
  1. Task name format: jobname_task
     >> example: job1533585500968-1533585495_task1
* Session:
  1. ManagementEngine launch the system once

* Job Definition(sample):
  ```json
   {
      "job_name": "job1533585500968-1533585495",
      "job_info": {
        "network": {
          "name": "RESTfulSwarmNetwork",
          "driver": "overlay",
          "subnet": "129.59.0.0/16"
        },
        "tasks": {
          "job1533585500968-1533585495_task1": {
            "container_name": "job1533585500968-1533585495_task1",
            "node": "",
            "image": "image_name",
            "detach": true,
            "command": "",
            "req_cores": 2,
            "cpuset_cpus": "",
            "mem_limit": "10m",
            "ports": {"3000/tcp": 3000},
            "volumes": {},
            "environment": {},
            "status": "Ready"
          }
        }
      },
      "status": "Ready",
      "start_time": 0,
      "end_time": 0
   }  
  ```  

* Define arguments for actors(required arguments for running actor script):
    ```json
     {
        "FE": {
            "address": "129.0.0.1"
        },
        "JM": {
            "address": "129.0.0.2",
            "wait_time": 0.1,
            "scheduling_strategy": {
              "best-fit": 1,
              "first-fit": 0,
              "first-fit-decreasing": 0,
              "best-fit-decreasing": 0,
              "no-scheduler": 0
            }
        },
        "GM": {
            "address": "129.0.0.3"
        },
        "DC": {
            "address": "129.0.0.4"
        }
     }
    ```

* Start System in Experimental mode (FE, JM, GM and DC are in same node):
    1. Start ManagementEngine
        ```bash
        python3 Management.py
        ``` 
    2. Start worker
        ```bash
         # frequency: time interval of sending containers status to DC
         python3 Worker.py $frequency
        ```

## Architecture

![Architecture](./Images/Architecture.jpg)

### SC--StressClient (Job# vs. Time)

* Role: Generate test data and feed data into the system
* SC types:
  1. Steady Stress Client
     * F(t) = a, a is steady constant
  2. Bursty Stress Client
     * Exponential distribution, with configurable constant lambda
  3. Incremental Stress Client
     * F(t) = at + b, a and b are constant
  4. Random Stress Client
     * F(t) = a, a is random constant

### FE--FrontEnd (RESTful)

* Role: Receive data from Stress Client and initialize Job collection in MongoDB
* Swagger Interface

![FrontEnd](./Images/FrontEnd.PNG)

### JM--JobManager

* Role: 
    1. Send RESTful call to GM
    2. Apply scheduling strategy on requested & free resources to get scheduling and waiting decision
    3. Maintain a job queue to buffer waiting jobs
    4. Update MongoDB when job info changed
* Provided Functions:
    1. Deploy job (POST)
    2. Deploy single task (POST)
    3. Migrate task (POST)
    4. Migrate job (POST)
    5. Update task (POST)
    6. Checkpoint task (POST)
    7. Remove worker in Swarm mode (GET)
    8. Describe Worker nodes (GET)
    9. Describe GM (GET)
* Scheduling Strategies
    1. Bin Packing
       * Best Fit
       * First Fit
       * Best Fit Decreasing
       * First Fit Decreasing
    2. Node Scheduling
       * Client specify worker node and cpuset_cpus information
    3. Priority Scheduling (in-progress)

* Job state chart

![JobState](./Images/JobState.jpg)
  
### GM--GlobalManager (RESTful)

* Role:
  1. Notify worker nodes to conduct commands passed from JM
  2. Initialize Swarm environment and play the role of master node
  3. Update MongoDB
  4. Periodically prune unused resources in swarm cluster
  5. NFS master

* Swagger Interface

![GlobalManager](./Images/GlobalManager.PNG)

### Woker

* Role:
  1. Worker node in Swarm mode
  2. Deploy tasks
  3. Periodically collect finished tasks info and notify Discovery to update task status
  4. NFS client

### DIS--Discovery

* Role:
  1. Received notification from Workers to update Job and tasks status

### ME--ManagementEngine

* Role:
  1. Launch SC, FE, JM, GM, DIS, ME as separate processes in one VM/PM
  2. Launch all remote workers using SSH
  3. Terminate processes running in either local machine or remote worker machines

### MongoDB Interface
![WorkersInfo](./Images/WorkersInfo.JPG)
![JobInfo](./Images/JobInfo.JPG)
![WorkersResourceInfo](./Images/WorkersResourceInfo.JPG)

Note: StressClient, Discovery, FrontEnd and JobManager have been dockerrized.