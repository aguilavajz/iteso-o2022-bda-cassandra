# iteso-o2022-bda-cassandra

A place to share cassandra app code

### Setup a python virtual env with python cassandra installed
```
# If pip is not present in you system
sudo apt update
sudo apt install python3-pip

# Install and activate virtual env
python3 -m pip install virtualenv
virtualenv -p python3 ./venv
source ./venv/bin/activate

# Install project python requirements
python3 -m pip install -r requirements.txt
```


### Launch cassandra container
```
docker start cassandra || docker run --name cassandra -d cassandra
```

### Copy data to container
```
docker cp tools/data.cql cassandra:/root/data.cql
docker exec -it cassandra bash -c "cqlsh -u cassandra -p cassandra"
#In cqlsh:
USE investments;
SOURCE "/root/data.cql"
```

### Start a Cassandra cluster with 2 nodes
```
# Recipe to create a cassandra cluster using docker
docker run --name node1 -d cassandra:latest
docker run --name node2 -d --link node1:cassandra cassandra:latest

# Wait for containers to be fully initialized, verify node status
docker exec -it node1 nodetool status
```
