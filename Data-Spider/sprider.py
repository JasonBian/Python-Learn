#!/usr/bin/python -*- coding: utf-8 -*- #
import requests, sys, re
from bs4 import BeautifulSoup

reload(sys)
sys.setdefaultencoding('utf-8')
print '正在从豆瓣电影Top250抓取数据......'

for page in range(10):
    url = 'https://movie.douban.com/top250?start=' + str((page - 1) * 25)
    print '---------------------------正在爬取第' + str(page + 1) + '页......--------------------------------'
    html = requests.get(url)
    html.raise_for_status()
    try:
        soup = BeautifulSoup(html.text, 'html.parser')
        soup = str(soup)  # 利用正则表达式需要将网页文本转换成字符串
        title = re.compile(r'<span class="title">(.*)</span>')
        names = re.findall(title, soup)
        for name in names:
            if name.find('/') == -1:  # 剔除英文名(英文名特征是含有'/')
                print name
    except Exception as e:
        print e
print '爬取完毕！'
