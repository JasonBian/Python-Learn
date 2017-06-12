# coding=utf-8
# !/usr/bin/python

# ********************************************************************************
# get-yarn-usage.py
#
# Usage: ./get-hive-usage-user.py <rm_host> <cm_host> <cm_login> <cm_password> <day_interval> <filter_str>
#
# Edit the settings below to connect to your Cluster
#
# ********************************************************************************

import sys
from datetime import datetime, timedelta
from cm_api.api_client import ApiResource
from yarn_api_client import ResourceManager
import time
import xlsxwriter

reload(sys)
sys.setdefaultencoding('utf-8')

# Get command line args
cm_port = "7180"
rm_host = None
cm_host = None
cm_login = None
cm_password = None
day_interval = 1
filter_str = ''
if len(sys.argv) == 7:
    rm_host = sys.argv[1]
    cm_host = sys.argv[2]
    cm_login = sys.argv[3]
    cm_password = sys.argv[4]
    day_interval = sys.argv[5]
    filter_str = sys.argv[6]
else:
    print " Usage:  ./get-hive-usage-user.py <rm_host> <cm_host> <cm_login> <cm_password> <day_interval> <filter_str>"
    quit(1)

# Connect to RM
monitor = ResourceManager(rm_host, 8088, 30)

# Connect to CM
print "\nConnecting to Cloudera Manager at " + cm_host + ":" + cm_port
api = ApiResource(cm_host, version=6, username=cm_login, password=cm_password)

# Get Cluster
cluster = None
clusters = api.get_all_clusters()
for c in clusters:
    cluster = c
    break
if cluster is None:
    print "\nError: Cluster not found"
    quit(1)

# Get YARN Service
yarn_service = None
service_list = cluster.get_all_services()
for service in service_list:
    if service.type == "YARN":
        yarn_service = service
        break
if yarn_service is None:
    print "Error: Could not locate YARN Service"
    quit(1)

now = datetime.now()
start = now - timedelta(days=int(day_interval))

print start

filterStr = filter_str

yarn_apps_response = yarn_service.get_yarn_applications(start, now, filter_str=filterStr, limit=1000)
yarn_apps = yarn_apps_response.applications
print len(yarn_apps)

monitorList = []
# Iterate over the jobs
for i in range(0, len(yarn_apps)):
    json_data = {}
    yarn_app = yarn_apps[i]
    appInfo = monitor.cluster_application(yarn_app.applicationId)

    json_data['applicationId'] = yarn_app.applicationId
    json_data['user'] = yarn_app.user
    json_data['total_task_num'] = yarn_app.attributes.get('total_task_num')
    json_data['maps_total'] = yarn_app.attributes.get('maps_total')
    json_data['reduces_total'] = yarn_app.attributes.get('reduces_total')

    if appInfo.data is not None:
        app = appInfo.data.get('app')
        if app is not None:
            json_data['state'] = app.get('state')
            json_data['finalStatus'] = app.get('finalStatus')
            json_data['vcoreSeconds'] = app.get('vcoreSeconds')
            json_data['memorySeconds'] = app.get('memorySeconds')
            json_data['allocatedMB'] = app.get('allocatedMB')
            json_data['allocatedVCores'] = app.get('allocatedVCores')
            json_data['startedTime'] = time.strftime("%Y-%m-%d %H:%M:%S",
                                                     time.localtime(app.get('startedTime') / 1000))
            json_data['finishedTime'] = time.strftime("%Y-%m-%d %H:%M:%S",
                                                      time.localtime(app.get('finishedTime') / 1000))
            json_data['elapsedTime'] = str(app.get('elapsedTime'))
    # Get the Hive SQL
    hive_query_string = yarn_app.attributes.get("hive_query_string", None)
    if hive_query_string is not None:
        json_data['hiveQueryString'] = hive_query_string.encode('utf-8')
    monitorList.append(json_data)

wb = xlsxwriter.Workbook(str(time.time()) + ".xlsx")
titleFormat = wb.add_format()
titleFormat.set_bold()
titleFormat.set_bg_color("orange")
titleFormat.set_font_size(12)
titleFormat.set_align("center")
titleFormat.set_align("vcenter")
titleFormat.set_border(1)

contentFormat = wb.add_format()

ws = wb.add_worksheet()

ws.write(0, 0, "User", titleFormat)
ws.write(0, 1, "ApplicationId", titleFormat)
ws.write(0, 2, "total_task_num", titleFormat)
ws.write(0, 3, "maps_total", titleFormat)
ws.write(0, 4, "reduces_total", titleFormat)
ws.write(0, 5, "state", titleFormat)
ws.write(0, 6, "finalStatus", titleFormat)
ws.write(0, 7, "vcoreSeconds", titleFormat)
ws.write(0, 8, "memorySeconds", titleFormat)
ws.write(0, 9, "allocatedMB", titleFormat)
ws.write(0, 10, "allocatedVCores", titleFormat)
ws.write(0, 11, "开始时间", titleFormat)
ws.write(0, 12, "结束时间", titleFormat)
ws.write(0, 13, "执行时间(单位:ms)", titleFormat)
ws.write(0, 14, "hive_query_string", titleFormat)

for i in range(len(monitorList)):
    ws.write(i + 1, 0, monitorList[i].get("user"), contentFormat)
    ws.write(i + 1, 1, monitorList[i].get("applicationId"), contentFormat)
    ws.write(i + 1, 2, monitorList[i].get("total_task_num"), contentFormat)
    ws.write(i + 1, 3, monitorList[i].get("maps_total"), contentFormat)
    ws.write(i + 1, 4, monitorList[i].get("reduces_total"), contentFormat)
    ws.write(i + 1, 5, monitorList[i].get("state"), contentFormat)
    ws.write(i + 1, 6, monitorList[i].get("finalStatus"), contentFormat)
    ws.write(i + 1, 7, monitorList[i].get("vcoreSeconds"), contentFormat)
    ws.write(i + 1, 8, monitorList[i].get("memorySeconds"), contentFormat)
    ws.write(i + 1, 9, monitorList[i].get("allocatedMB"), contentFormat)
    ws.write(i + 1, 10, monitorList[i].get("allocatedVCores"), contentFormat)
    ws.write(i + 1, 11, monitorList[i].get("startedTime"), contentFormat)
    ws.write(i + 1, 12, monitorList[i].get("finishedTime"), contentFormat)
    ws.write(i + 1, 13, monitorList[i].get("elapsedTime"), contentFormat)
    ws.write(i + 1, 14, monitorList[i].get("hiveQueryString"), contentFormat)
wb.close()
