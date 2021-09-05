import json
import uuid
import datetime

from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import SimpleStatement

hostname = "xxxxxxxxxxxxxxxx"
# hostname = "127.0.0.1"
username = "xxxxxxxxxxxxxxxx"
password = "xxxxxxxxxxxxxxxx"
keyspace = "dev_moni"

#adding my hostname to an array, setting up auth, and connecting to Cassandra
nodes = ['xxxxxxxxxxxxxxxx', 'xxxxxxxxxxxxxxxx']
port = '9042'
nodes.append(hostname)

def fetch_all_data_all_row_from_log(query_str):
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    try:
        cluster = Cluster(nodes, port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        #preparing and executing my INSERT statement
        strCQL = query_str
        pStatement = session.prepare(strCQL)
        # print(pStatement)
        all_row = session.execute(pStatement)
        
        row = []

        for each_row in all_row:
            json_form = json.loads(each_row[0])
            row.append(json_form)
        return row
    except:
        return "can't fetch DB"

def update_cassandra_db(query_str):
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    try:
        cluster = Cluster(nodes, port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)

        #preparing and executing my INSERT statement
        strCQL = query_str
        pStatement = session.prepare(strCQL)

        all_row = session.execute(pStatement)
        return "can update DB"

    except:
        return "can't update DB"

def gen_uuid_v5():
    uuid_4 = uuid.uuid4()
    uuid_v5 = uuid.uuid5(uuid.uuid4(), datetime.datetime.now().strftime('%S%f'))
    return uuid_v5

def insert_cassandra_db(query_str):
    auth_provider = PlainTextAuthProvider(username=username, password=password)
    try:
        cluster = Cluster(nodes, port=port, auth_provider=auth_provider)
        session = cluster.connect(keyspace)
        uuid_v5 = gen_uuid_v5()

        #preparing and executing my INSERT statement
        strCQL = query_str
        pStatement = session.prepare(strCQL)
        session.execute(pStatement)
        return "can insert DB"
    except:
       return "can't insert DB"

