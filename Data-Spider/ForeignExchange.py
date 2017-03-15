#!/usr/bin/python -*- coding: utf-8 -*- #
import requests, sys, re, urllib
from bs4 import BeautifulSoup
from datetime import *

reload(sys)
sys.setdefaultencoding('utf-8')

print '正在从ICBC抓取数据......'

# beginDate = str(date.today())
# endDate = str(date.today())
beginDate = sys.argv[1]
endDate = sys.argv[2]

print 'beginDate:' + beginDate
print 'endDate:' + endDate

baseurl = 'http://www.icbc.com.cn/ICBCDynamicSite/Optimize/Quotation/QuotationListIframe.aspx' \
      '?variety=2&beginDate=' + beginDate + '&endDate=' + endDate + '&currency=USD&ppublishDate='
currentPage = 1
count = 0
url = ''
while True:
    if url.__len__() < 1:
        url = baseurl
    wp = urllib.urlopen(url)
    content = wp.read()
    soup = BeautifulSoup(content, from_encoding='utf-8')
    nextPage = soup.find_all("a", text="【下一页】")

    strValue = ''
    print '币种' + '\t\t' + '日期' + '\t\t' + '现汇买入价' + '\t' + '现钞买入价' + '\t' + '现汇卖出价' + '\t' + '现钞卖出价' + '\t' + '发布时间'
    for i in soup.find_all('td', 'tdCommonTableData'):
        count += 1
        strValue += i.string
        strValue += '\t'
        if count > 2:
		strValue +='\t'
        if count == 7:
            print strValue
            count = 0
            strValue = ''

    if len(nextPage) > 0:
        currentPage += 1
        url = baseurl + '&pageEXCHANGE_CH=' + str(currentPage)

    else:
        break

print '爬取完毕！'
