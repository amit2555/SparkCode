#!/usr/bin/python

#-------------------------------------------------------------------------
# Spark code to validate every BGP prefix's origin without using RPKI.
# Invalid prefix will be filtered and dashboard alert will be raised.
#-------------------------------------------------------------------------

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from socketIO_client import SocketIO, BaseNamespace
from collections import defaultdict
from spark_helpers import * 
import netaddr


def update_dashboard(kwargs):
    socketIO = SocketIO('localhost', 8000)
    for device,contents in kwargs.items():
        for item in contents['data']:
            msg = "Invalid prefix {} originated by ASN {} detected on router {}.\t\tPrefix filtered: {}".format(item['prefix'],item['asn'],device,contents['device_changed'])
            socketIO.emit('broadcast_event',{'data':msg})

def check_exact_match(adv_prefix,db_prefixes):
    prefixes_from_db = [ db_prefix for db_prefix, _ in db_prefixes ]
    if adv_prefix in prefixes_from_db:
        return True	# Advertised prefix is an exact match.        
    else:
        return False

def check_subnet_match(adv_prefix, db_prefixes):
    db_prefix_subnets = list()

    for db_prefix, maxlen in db_prefixes:
        ip_list = list(netaddr.IPNetwork(db_prefix).subnet(int(maxlen)))
        db_prefix_subnets.extend(ip_list)        
    if netaddr.IPNetwork(adv_prefix) in db_prefix_subnets:
        return True	# Advertised prefix is a subnet of a prefix advertised by the originating ASN.
    else:
        return False

def validate_bgp_prefix(iter):
    to_be_actioned = defaultdict(list)

    with get_db_connection('openbmp') as db:
        for record in iter:
            originating_asn = record['Origin_AS']
            advertised_prefix = record['Prefix']
             
            db_result = db.ipam.find({'asn':originating_asn}) 
            db_items = [ item for item in db_result ]
            
            if db_items:
                db_prefixes_matched = [ (item['prefix'],item['maxlen']) for item in db_items ]

                exact_match = check_exact_match(advertised_prefix,db_prefixes_matched)
                if not exact_match:
                    subnet_match = check_subnet_match(advertised_prefix,db_prefixes_matched)
                    if not subnet_match:
                        to_be_actioned[record['Router_IP']].append({'prefix':advertised_prefix,'asn':originating_asn})
            else:
                to_be_actioned[record['Router_IP']].append({'prefix':advertised_prefix,'asn':originating_asn})

    if to_be_actioned:
        filtered = do_filter_invalid_prefix_by_asn(to_be_actioned)
        update_dashboard(filtered)

def main():
    brokers = 'localhost:9092'
    topic = 'openbmp.parsed.unicast_prefix'
    sc = SparkContext(appName='BGPPrefixOriginValidation')
    ssc = StreamingContext(sc,2)
 
    directKafkaStream = KafkaUtils.createDirectStream(ssc, [topic], {'metadata.broker.list':brokers})
    #directKafkaStream.pprint()

    lines = directKafkaStream.flatMap(lambda x: x[1].splitlines()).filter(lambda line: line.startswith('add'))
    structured_rdd = lines.map(structure_data)
 
    structured_rdd.foreachRDD(lambda rdd: rdd.foreachPartition(validate_bgp_prefix)) 
    
    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
