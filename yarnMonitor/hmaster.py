#!/bin/env python2.7

import sys
import requests
import json
import socket
import argparse

names = ['*']
metrics = {}

excludes = [
            'volumeinfo',
            'version',
            'modelertype',
            'tag.port',
            'tag.context',
            'tag.hostname',
            'tag.sessionid',
            'tag.processname'
           ]

for n in names:
    url = 'http://{0}:60010/jmx?qry=Hadoop:service=HBase,name={1}'.format(socket.getfqdn(), n)    
    resp = requests.get(url, timeout=60).json()['beans']
    for i in range(len(resp)):
        for k, v in resp[i].iteritems():
            if k.lower() not in excludes:
                metrics[k] = v

if __name__ == '__main__':
   parser = argparse.ArgumentParser(description='Hadoop HBase metrics collector for Zabbix.')
   parser.add_argument('-t','--type',required=True)
   args = parser.parse_args()
   print metrics[args.type]
