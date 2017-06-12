# coding=utf-8
# !/usr/bin/python
from yarn_api_client import ResourceManager
from sympy import *
import time
import os
import json
import urllib2
import sys
import datetime

values = json.dumps(
    {"status": -1, "taskId": "ClusterMetricsMonitorForYunying", "contentDate": "2017-05-17 16:42:03", "context": {},
     "msg": "Success", "runTime": "2017-05-17 16:42:03", "data": {"usage": 0.6648277595029239}})
req = urllib2.Request('http://172.16.11.225:9090/action/receiver.do', values)
response = urllib2.urlopen(req, timeout=10 * 60)
status = response.read()
