# -*- coding: utf-8 -*-

import time, threading


# 线程代码
class TaskThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self, name=name)

    def run(self):
        print('thread %s is running...' % self.getName())
        for i in range(6):
            print('thread %s >>> %s' % (self.getName(), i))
            time.sleep(1)
        print('thread %s finished.' % self.getName())


taskthread = TaskThread('TaskThread')
taskthread.start()
taskthread.join()
