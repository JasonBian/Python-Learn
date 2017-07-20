# coding=utf-8
# !/usr/bin/python

from yarn_api_client import ResourceManager
import time
import json
import urllib2
import sys
import datetime
import logging

if len(sys.argv) == 3:
    rm_host = sys.argv[1]
    rm_host_2 = sys.argv[2]
else:
    print " Usage:  ./yarnmonitor.py <rm_host> <rm_host_2>"
    quit(1)

# Send email url
Url = "http://localhost/action/receiver.do"

pendingAppsDict = {}

pendingAppsTotalCount = 0
activeAppsTotalCount = 0
start = time.time()
warningcount = 0


def warning_process(pendingAppsDict):
    pendingcount = 0
    runningcount = 0
    pendingcount += pendingAppsDict['root.dm.numPendingApps']
    pendingcount += pendingAppsDict['root.ad.numPendingApps']
    pendingcount += pendingAppsDict['root.gz.numPendingApps']

    runningcount += pendingAppsDict['root.dm.numActiveApps']
    runningcount += pendingAppsDict['root.ad.numActiveApps']
    runningcount += pendingAppsDict['root.gz.numActiveApps']

    print pendingcount, runningcount

    percentage = 0.0
    if runningcount != 0:
        percentage = pendingcount / runningcount
    print percentage
    if percentage > 0.6:
        return True
    else:
        return True


if __name__ == '__main__':
    last_warning_time = None
    while True:
        end = time.time()
        if int(end - start) >= 86400:
            logging.error("Exit the process")
            sys.exit(0)
        else:
            # noinspection PyBroadException
            try:
                # Connect to RM
                monitor = ResourceManager(rm_host, 8088, 30)
                logging.error('Connect to ' + rm_host)
                scheduler = monitor.cluster_scheduler()
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
                logging.error(pendingAppsTotalCount)
                logging.error(activeAppsTotalCount)
                if warning_process(pendingAppsDict) and pendingAppsTotalCount >= 0:
                    json_data = {}
                    endTime = datetime.datetime.now()
                    endTimestamp = int(time.mktime(endTime.timetuple()) * 1000)
                    json_data['taskId'] = "MonitorPendingAppsForFenxi"
                    json_data['status'] = -1
                    json_data['msg'] = 'Success'
                    json_data['runTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
                    json_data['contentDate'] = str(
                        time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endTimestamp / 1000)))
                    json_data['context'] = {}
                    json_data['data'] = pendingAppsDict
                    values = json.dumps(json_data)
                    print values
                    req = urllib2.Request(Url, values)
                    response = urllib2.urlopen(req, timeout=10 * 60)
                    status = response.read()
                    now = time.time()
                    if last_warning_time is not None:
                        period = now - last_warning_time
                        print period
                        print warningcount
                        if warningcount == 1 and (30 <= period <= 200):
                            last_warning_time = now
                            time.sleep(180)
                            warningcount += 1
                            print 'Second warning'
                        elif warningcount == 2 and (180 <= now - last_warning_time <= 400):
                            last_warning_time = now
                            time.sleep(360)
                            print 'warning'
                        else:
                            warningcount = 0
                            last_warning_time = None
                            time.sleep(30)
                    else:
                        print 'First warning'
                        time.sleep(30)
                        warningcount += 1
                        last_warning_time = now
                pendingAppsTotalCount = 0
                activeAppsTotalCount = 0
            except:
                logging.error("Exception raised.")
                if rm_host == rm_host:
                    rm_host = rm_host_2
                else:
                    rm_host = rm_host
                continue
