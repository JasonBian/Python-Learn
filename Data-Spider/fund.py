# coding=utf-8
# !/usr/bin/python

from bs4 import BeautifulSoup as BS
import requests
import os
import re
import threading
import thread
import queue
import csv

import sys

reload(sys)
sys.setdefaultencoding('utf-8')

url = 'http://fund.eastmoney.com/allfund.html#0'


def html_download(self, url):
    if url is None:
        return None

    try:
        response = requests.get(url)
    except Exception as e:
        print "Open the url failed, error: {}".format(e)
        return None
    if response.status_code != 200:
        return None
    return response.content


def html_extract_content(self, html_cont):
    funds_text = []
    if html_cont is None:
        return
    soup = BS(html_cont, 'html.parser', from_encoding='gb18030')
    '''get all the fund id '''
    title_node = soup.title
    print title_node.getText()

    uls = soup.find_all('ul', class_='num_right')
    for ul in uls:
        for each in ul.find_all('li'):
            li_list = each.find_all('a')
            fund_info_dict = {'fund_id': '',
                              'fund_name': '',
                              'fund_url': ''}
            if len(li_list) > 1:
                fund = li_list[0].text
                fund_id = re.findall(r'\d+', fund)[0]
                fund_url = li_list[0].attrs['href']
                fund_name = fund.decode('utf-8')[fund.find(ur')') + 1:].encode('utf8')
                fund_info_dict['fund_id'] = fund_id
                fund_info_dict['fund_name'] = fund_name
                fund_info_dict['fund_url'] = fund_url
                funds_text.append(fund_info_dict)
    return funds_text


def parse_url(self, url):
    html_content = self.html_download(url)
    return self.html_extract_content(html_content)


if __name__ == '__main__':
    print html_extract_content(html_download(url))


class Handle_Url(Thread):
    All_Funds = []

    def __init__(self, queue):
        super(Handle_Url, self).__init__()
        self.queue = queue

    def run(self):
        print 'run in Parse_url'
        while True:
            if self.queue.empty():
                break
            else:
                fund = self.queue.get()
                url = fund['fund_url']
                fund_id = fund['fund_id']
                fund_name = fund['fund_name']
                print self.name + ":" + "Begin parse %s now" % url
                fund_data = self.parse_url(url)
                fund_data.insert(0, fund_id)
                fund_data.insert(1, fund_name)

    @staticmethod
    def write_csv_head():
        with open(CSV_FILE, 'ab') as wf:
            head = ['fund_id', 'fund_name', 'one_month', 'one_year', 'three_month', 'three_year', 'six_month',
                    'from_start']
            writer = csv.writer(wf)
            writer.writerow(head)

    @staticmethod
    def write_each_row_in_csv(self, text):
        with open(CSV_FILE, 'ab') as wf:
            writer = csv.writer(wf)
            writer.writerow(text)
