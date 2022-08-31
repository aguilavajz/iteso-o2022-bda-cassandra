
Setup a python virtual env with python cassandra installed
'''
python3 -m pip install virtualenv
virtualenv -p python3 ./venv
source ./venv/bin/activate
python3 -m pip install python-cassandra
'''


Launch cassandra container
'''
docker start cassandra || docker run --name cassandra -d cassandra
'''

Copy data to container
'''
docker cp data.cql cassandra:/root/data.cql
docker exec -it cassandra bash -c "cqlsh -u cassandra -p cassandra"
#In cqlsh:
USE investments;
SOURCE "~/data.cql"
'''
