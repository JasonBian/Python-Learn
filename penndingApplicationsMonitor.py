# coding=utf-8
# !/usr/bin/python

from yarn_api_client import ResourceManager
import time
import json
import urllib2
import sys
import datetime

if len(sys.argv) == 3:
    rm_host = sys.argv[1]
    # Send email url
    email_url = sys.argv[2]
else:
    print " Usage:  ./pendingApplicationsMonitor.py <rm_host> <emailService_url>"
    quit(1)

# Connect to RM
monitor = ResourceManager(rm_host, 8088, 30)

pendingAppsDict = {}

pendingAppsTotalCount = 0
activeAppsTotalCount = 0
while True:
    # noinspection PyBroadException
    try:
        scheduler = monitor.cluster_scheduler()
        # print scheduler.data
        if scheduler.data is not None:
            schedulerData = scheduler.data.get('scheduler')
            if schedulerData is not None:
                schedulerInfo = schedulerData.get('schedulerInfo')
                if schedulerInfo is not None:
                    rootQueue = schedulerInfo.get('rootQueue')
                    if rootQueue is not None:
                        childQueues = rootQueue.get('childQueues')
                        if childQueues is not None:
                            for childQueue in childQueues:
                                queueName = childQueue.get('queueName')
                                numPendingApps = childQueue.get('numPendingApps')
                                numActiveApps = childQueue.get('numActiveApps')
                                pendingAppsDict[queueName + '.numPendingApps'] = numPendingApps
                                pendingAppsDict[queueName + '.numActiveApps'] = numActiveApps
                                pendingAppsTotalCount += numPendingApps
                                activeAppsTotalCount += numActiveApps
        pendingAppsDict['pendingAppsTotalCount'] = pendingAppsTotalCount
        pendingAppsDict['activeAppsTotalCount'] = activeAppsTotalCount
        print pendingAppsDict
        if pendingAppsTotalCount > 0:
            json_data = {}
            endTime = datetime.datetime.now()
            endTimestamp = int(time.mktime(endTime.timetuple()) * 1000)
            json_data['taskId'] = "MonitorPendingApps"
            json_data['status'] = -1
            json_data['msg'] = 'Success'
            json_data['runTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
            json_data['contentDate'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
            json_data['context'] = {}
            json_data['data'] = pendingAppsDict
            values = json.dumps(json_data)
            print values
            req = urllib2.Request(email_url, values)
            response = urllib2.urlopen(req, timeout=10 * 60)
            status = response.read()
            print status
        pendingAppsTotalCount = 0
        activeAppsTotalCount = 0
        time.sleep(60)
    except:
        print 'Exception raised.'
        continue
