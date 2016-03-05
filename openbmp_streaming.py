#!/usr/bin/python

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pymongo import MongoClient
import netaddr
from contextlib import contextmanager
from automation import tasks


@contextmanager
def connect_to_db(dbase):
    conn = MongoClient('mongodb://localhost',port=27017)
    db = conn[dbase]
    yield db
    conn.close()

def check_for_hijack(record_entry):
    our_ranges = ['1.0.0.0/8','172.16.0.0/16']
    our_asn = '300'
    if record_entry['Action'] == 'add': 
        prefix = record_entry['Prefix']
        prefixlen = record_entry['PrefixLen']
        origin_asn = record_entry['Origin_AS']
        as_path = record_entry['AS_Path'].lstrip().split()
        router = record_entry['Router_IP']
        match_found = netaddr.all_matching_cidrs(prefix,our_ranges)
        if match_found and our_asn != origin_asn and our_asn not in as_path:
            prefix = prefix + '/' + prefixlen
            message = 'Detected by router {}, prefix {} appers to be hijacked or incorrectly leaked.'.format(router,prefix) 
            mitigated = tasks.apply_filter_config(router,prefix)
            with connect_to_db('openbmp') as db:
                db.alerts.insert({'timestamp':record_entry['Timestamp'],'message':message,'originating_asn':origin_asn,'as_path':record_entry['AS_Path'],'mitigated':mitigated}) 
            
def sendPartition(iter):
    for record in iter:
        check_for_hijack(record)
        with connect_to_db('openbmp') as db:
            db.unicast_prefix.insert(record)

def structure_data(line):
    fields = ['Action',
    'Sequence',
    'Hash',
    'Router_Hash',
    'Router_IP',
    'Base_Attr_Hash',
    'Peer_Hash',
    'Peer_IP',
    'Peer_ASN',
    'Timestamp',
    'Prefix',
    'PrefixLen',
    'isIPv4',
    'Origin',
    'AS_Path',
    'AS_Path_Count',
    'Origin_AS',
    'Next_Hop',
    'MED',
    'Local_Pref',
    'Aggregator',
    'Community_List',
    'Ext_Community_List',
    'Cluster_List',
    'isAtomicAgg',
    'isNextHopIPv4',
    'Originator_Id']

    d = dict()
    for k,v in zip(fields,line.split('\t')):
        d[k] = str(v)
    return d


sc = SparkContext(appName="OpenBMPViaKafka")
ssc = StreamingContext(sc,5)

zkQuorum = 'localhost:2181'
topic = 'openbmp.parsed.unicast_prefix'
partition = 1
kafkaStream = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: partition})
#kafkaStream.pprint()

lines = kafkaStream.flatMap(lambda x: x[1].splitlines()).filter(lambda line: line.startswith('add') or line.startswith('del'))

result = lines.map(structure_data)
#result.pprint()

result.foreachRDD(lambda rdd: rdd.foreachPartition(sendPartition))

ssc.start()
ssc.awaitTermination()

