# coding=utf-8
import time
import json
import sys
import urllib2

reload(sys)
import os
from yarn_api_client import ResourceManager
import datetime

if __name__ == '__main__':
    Url = "http://localhost/action/receiver.do"
    endtime = datetime.datetime.now()
    starttime = endtime - datetime.timedelta(days=7)

    print starttime, endtime
    starttimestamp = int(time.mktime(starttime.timetuple()) * 1000)
    endtimestamp = int(time.mktime(endtime.timetuple()) * 1000)
    print starttimestamp, endtimestamp
    try:  # 访问yarn取得任务信息
        monitor = ResourceManager("172.16.17.115", 8088, 30)
        out = monitor.cluster_applications(None, None, None, None, None, None, None, str(starttimestamp),
                                           str(endtimestamp))
    except:
        monitor = ResourceManager("172.16.17.115", 8088, 30)
        out = monitor.cluster_applications(None, None, None, None, None, None, None, str(starttimestamp),
                                           str(endtimestamp))
    if out.data['apps'] is None:
        print 'error'
        exit()
    applicationlist = out.data['apps']['app']
    excutorMap = {}  # 存储每个user的消耗资源总量
    for applicationinfo in applicationlist:
        user = applicationinfo['user']
        vcoreSeconds = int(applicationinfo['vcoreSeconds'])
        if excutorMap.has_key(user):
            excutorMap[user] = excutorMap[user] + vcoreSeconds
        else:
            excutorMap[user] = vcoreSeconds
    # 发送请求
    json_data = {}
    json_data['taskId'] = "yarnStatistics-yunying"
    json_data['status'] = -1
    json_data['msg'] = 'Scuess'
    json_data['runTime'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endtimestamp / 1000)))
    json_data['contentDate'] = str(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(endtimestamp / 1000)))
    json_data['context'] = {}
    data = excutorMap
    json_data['data'] = data
    values = json.dumps(json_data)
    print json_data
    req = urllib2.Request(Url, values)
    response = urllib2.urlopen(req, timeout=10 * 60)
    status = response.read()
