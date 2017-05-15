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

if len(sys.argv) == 4:
    rm_host = sys.argv[1]
    file_path = sys.argv[2]
    email_url = sys.argv[2]
else:
    print " Usage:  ./ClusterMetricsMonitor.py <rm_host> <file_path> <email_url>"
    quit(1)


def do_integrate(x1, y1, x2, y2):
    k = (y2 - y1) / (x2 - x1)
    b = y1 - k * x1
    x = symbols('x')
    return float(integrate(k * x + b, (x, x1, x2)))


file_path += '_' + time.strftime('%Y%m%d', time.localtime(time.time())) + '.data'
if os.path.isfile(file_path):
    os.remove(file_path)
# 追加写文件
f = open(file_path, 'a')

count = 1
availableMBList = []  # yarn cluster availableMB
while count < 145:
    # noinspection PyBroadException
    try:
        # Connect to RM
        monitor = ResourceManager(rm_host, 8088, 30)
        metrics = monitor.cluster_metrics()
        print count
        print metrics.data
        if metrics.data is not None:
            clusterMetric = metrics.data.get('clusterMetrics')
            if clusterMetric is not None:
                f.write(str(count) + ',' + str(clusterMetric.get('availableMB')) + ',' + str(
                    time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(
                        int(time.mktime(datetime.datetime.now().timetuple()) * 1000) / 1000))) + "\n")
                f.flush()
        count += 1
        time.sleep(1)  # 每10分钟获取一次cluster_metrics
    except:
        print 'get cluster_metrics error'
        continue

f.close()

f1 = open(file_path, 'r')
availableMBList = f1.readlines()
fileLineCount = availableMBList.__len__()
print fileLineCount

availableMBSum = 0.0
for num in range(1, fileLineCount):
    availableMBSum += do_integrate(int(str(availableMBList[num - 1]).split(',')[0]),
                                   int(str(availableMBList[num - 1]).split(',')[1]),
                                   int(str(availableMBList[num]).split(',')[0]),
                                   int(str(availableMBList[num]).split(',')[1]))
f1.close()

totalMB = 1.0  # Total Memory of the YARN Cluster
# noinspection PyBroadException
try:
    monitor = ResourceManager(rm_host, 8088, 30)
    cluster_metrics = monitor.cluster_metrics()
    if cluster_metrics.data is not None:
        yarnMetrics = cluster_metrics.data.get('clusterMetrics')
        if yarnMetrics is not None:
            totalMB = float(yarnMetrics.get('totalMB'))
except:
    print 'get cluster totalMB error'
percentageOfMB = availableMBSum / (totalMB * fileLineCount)
print percentageOfMB
endTime = datetime.datetime.now()
endTimestamp = int(time.mktime(endTime.timetuple()) * 1000)
json_data = {}
if percentageOfMB < 0.4:
    json_data['taskId'] = 'ClusterMetricsMonitor'
    json_data['status'] = -1
    json_data['msg'] = 'Success'
    json_data['runTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
    json_data['contentDate'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
    json_data['context'] = {}
    json_data['data'] = {'usage': (1.0 - percentageOfMB)}
    values = json.dumps(json_data)
    print values
    req = urllib2.Request(email_url, values)
    response = urllib2.urlopen(req, timeout=10 * 60)
    status = response.read()
