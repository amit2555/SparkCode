#!/usr/bin/python

from pymongo import MongoClient
from contextlib import contextmanager
from automation import tasks
import multiprocessing as mp


def do_filter_invalid_prefix_by_asn(parsed_dict):
    jobs = list()
    manager = mp.Manager()
    result_dict = manager.dict()
    
    for device, contents in parsed_dict.iteritems():
        job = mp.Process(target=tasks.filter_invalid_prefix_by_asn,args=(device,contents,result_dict))
        job.start()
        jobs.append(job)

    for job in jobs:
        job.join()

    return result_dict

@contextmanager
def get_db_connection(dbase):
    conn = MongoClient('mongodb://localhost',port=27017)
    db = conn[dbase]
    yield db
    conn.close()

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
    d['Prefix'] = d['Prefix'] + '/' + d['PrefixLen']
    d.pop('PrefixLen')
    return d


