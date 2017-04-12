# -*- coding: utf-8 -*-

from Queue import Queue
import random, threading, time


# 生产者类
class Producer(threading.Thread):
    def __init__(self, name, queue):
        threading.Thread.__init__(self, name=name)
        self.data = queue

    def run(self):
        for i in range(5):
            print("%s is producing %d to the queue!" % (self.getName(), i))
            self.data.put(i)
            time.sleep(random.randrange(10) / 5)
        print("%s finished!" % self.getName())


# 消费者类
class Consumer(threading.Thread):
    def __init__(self, name, queue):
        threading.Thread.__init__(self, name=name)
        self.data = queue

    def run(self):
        for i in range(5):
            val = self.data.get()
            print("%s is consuming. %d in the queue is consumed!" % (self.getName(), val))
            time.sleep(random.randrange(10))
        print("%s finished!" % self.getName())


def main():
    queue = Queue()
    producer = Producer('Producer', queue)
    consumer = Consumer('Consumer', queue)
    producer.start()
    consumer.start()
    producer.join()
    consumer.join()
    print 'All threads finished!'


if __name__ == '__main__':
    main()
