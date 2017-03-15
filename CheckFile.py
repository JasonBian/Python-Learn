# encoding=utf-8

import urllib
import urllib2
import json
import re
import os
import time
import sys


# 参数举例: (http://192.168.2.159:9090/action/SyncReceiver.do;Type14SizeCheck;2016-02-19;hdfs://192.168.10.84:8020/user/zhouqf/type14/applistType14.txt,sizeOfType14)
# 参数之间以";"分隔
# 第一个参数是服务的url
# 第二个参数是task_id
# 第三个参数以后的每一个参数代表  一个输入目录,一个参数名


# 解析参数
def parse_paras(paras):
    reg = "(\()(.*)(\))"
    m = re.match(reg, paras)
    arr = m.group(2)
    data = arr.split(";")
    url = data[0]
    task_id = data[1]
    log_date = data[2]
    paras_type = data[3:]
    paras_type = [i.split(",") for i in paras_type]
    return url, task_id, log_date, paras_type


# 文件目录检查
def Dir_File_Check(input):
    cmd3 = "hdfs dfs -test -d %s" % input
    cmd1 = "hdfs dfs -test -e %s" % input
    cmd2 = "hdfs dfs -test -e %s._COPYING_" % input
    cmd4 = "hdfs dfs -ls %s |awk '{print $8}'|grep _COPYING_" % input
    cmd5 = "hdfs dfs -ls %s" % input
    status1 = os.system(cmd1)
    status2 = os.system(cmd2)
    status3 = os.system(cmd3)
    print status1, status2, status3
    if status1 != 0:
        # 如果对应的目录或者文件名不存在,且带有_COPYING_的文件也不存在,那么抛出错误,直接退出程序
        if status2 != 0:
            print "%s don't exists,please check the input name " % input
            sys.exit(1)
        # 如果文件不存在,但是._COPYING_文件存在,说明当前正在传输这个文件,等待它上传完毕就好
        else:
            print "%s is in transmission ,please wait for a moment" % input
            while True:
                if os.system(cmd2) == 0:
                    time.sleep(5)
                    print "please wait for a moment,the file is in transmission"
                else:
                    print "%s has uploaded,you can use it securely" % input
                    break
    else:
        # 如果对应的文件和目录存在,则判断它是文件还是目录,如果是目录,检查目录下是否在存在._COPYING_文件
        if status3 == 0:
            if os.popen(cmd5).readlines() == []:
                print "the file in the %s is None" % input
                sys.exit(1)
            while os.popen(cmd4).readlines() != []:
                print "files in %s are in transmission,please wait for a moment" % input
                time.sleep(1)

            print "files in %s has been uploaded,you can use it securely" % input
        # 如果是文件,就说明已经上传完毕
        else:
            print "the file of %s has been uploaded,you can use it securely" % input
            return


# 获取文件大小,以字节(bytes)表示
def Get_Size(input):
    cmd = "hdfs dfs -du -s %s | awk '{print $1}'" % input
    return os.popen(cmd).readlines()[0].strip()


# 构造post请求数据
def Cons_Json_Data(paras):
    json_data = {}
    url, task_id, log_date, paras_type = parse_paras(paras)
    time_now = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    json_data['taskId'] = task_id
    json_data['status'] = -1
    json_data['msg'] = '任务运行成功'
    json_data['runTime'] = time_now
    json_data['contentDate'] = log_date
    json_data['context'] = {}
    data = {}
    for i in paras_type:
        size = Get_Size(i[0])
        data[i[1]] = size
    json_data['data'] = data
    return json_data


# 通过服务检查数据
def TestJsonData(url, json_data):
    values = json.dumps(json_data)
    req = urllib2.Request(url, values)
    response = urllib2.urlopen(req, timeout=15 * 60).read()
    status = json.loads(response)
    for i in status.keys():
        print i, ": ", status[i]
    level = status['level']
    if level == 0:
        print "the checking process is : correct"
    else:
        print "the checking process is : wrong ,please check the program and retry it"
        sys.exit(1)


# 检查文件大小
def Check_File_Size(url, json_data):
    values = json.dumps(json_data)
    req = urllib2.Request(url, values)
    response = urllib2.urlopen(req, timeout=10 * 60)
    status = response.read()
    status = json.loads(status)
    status = status.level
    if status == 0:
        print "the size of the directory or file is : correct"
        return
    else:
        print "the size of the directory or file is : wrong"
        sys.exit(1)


if __name__ == "__main__":
    # json_data={
    #     "context": {},
    #     "data": {
    #         "user_app_actived_out":160119,
    #         "type6_raw_in":3346735174
    #     },
    #     "taskId": "AppActivedAddDidApp",
    #     "msg": "任务成功运行。",
    #     "status": -1,
    #     "runTime": "2016-02-17 17:37:40",
    #     "contentDate": "2016-02-18"
    # }
    input_data = sys.argv[1]
    # input_para = "(http://192.168.2.159:9090/action/SyncReceiver.do;Type14SizeCheck;2016-02-19;hdfs://192.168.10.84:8020/user/zhouqf/type14/applistType14.txt,sizeOfType14)"
    para = input_data
    url, task_id, log_date, paras_type = parse_paras(para)
    for i in paras_type:
        Dir_File_Check(i[0])
    json_data = Cons_Json_Data(paras=para)
    for k in json_data.keys():
        print k, " : ", json_data[k]
    TestJsonData(url, json_data)
