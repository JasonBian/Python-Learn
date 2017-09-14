# coding:utf-8

import scrapy
import re
import os
import sqlite3
from ..items import ZolspiderItem
import sys

reload(sys)
sys.setdefaultencoding('utf-8')


class ZolSpider(scrapy.Spider):
    name = "zol"
    # allowed_domains = ["http://detail.zol.com.cn/"]  # 用于限定爬取的服务器域名
    start_urls = [
        # 主要爬去中关村在线的手机信息页面，考虑到是演示目的就仅仅爬了首页，其实爬分页跟二级爬虫原理相同，出于节省时间目的这里就不爬了
        # 这里可以写多个入口URL
        "http://detail.zol.com.cn/cell_phone_index/subcate57_list_1.html"
    ]
    item = ZolspiderItem()  # 没法动态创建，索性没用上，用的meta在spider函数间传值
    # 只是test一下就用sqlite吧，比较轻量化
    # database = sqlite3.connect(":memory:")
    database_file = os.path.dirname(os.path.abspath(__file__)) + "\\phonedata.db"
    if os.path.exists(database_file):
        os.remove(database_file)
    database = sqlite3.connect(database_file)
    # 先建个字段，方便理解字段含义就用中文了
    database.execute(
        '''
        CREATE TABLE CELL_PHONES
        (
        手机型号 TEXT
        );
        '''
    )
    # 用于检查数据增改是否全面，与total_changes对比
    counter = 0

    # 手机报价首页爬取函数
    def parse(self, response):
        # 获取手机详情页链接并以其创建二级爬虫
        hrefs = response.xpath("//h3/a")
        for href in hrefs:
            url = response.urljoin(href.xpath("@href")[0].extract())
            yield scrapy.Request(url, self.parse_detail_page)

    # 手机详情页爬取函数
    def parse_detail_page(self, response):
        # 通过xpath获取手机型号
        model = response.xpath("//h1").xpath("text()")[0].extract()
        # 创建该型号手机的数据库记录
        sql = 'INSERT INTO CELL_PHONES (手机型号) VALUES ("' + model + '")'
        self.counter += 1
        self.database.execute(sql)
        self.database.commit()
        # 获取参数详情页的链接
        param_tag = u'参数'
        url = response.urljoin(
            response.xpath("//div[@id='tagNav']//a[text()='%s']" % param_tag).xpath("@href")[0].extract())
        # 由于Scrapy是异步驱动的（逐级启动爬虫函数），所以当需绑定父子级爬虫函数间的某些变量时，可以采用meta字典传递，全局的item字段无法动态创建，在较灵活的爬取场景中不是很适用
        yield scrapy.Request(url, callback=self.parse_param_page, meta={'model': model})

    def chinese_print(self, obj):
        return str(re.sub(r"\\u([a-f0-9]{4})", lambda mg: unichr(int(mg.group(1), 16)), obj.__repr__()))

    # 手机参数详情页爬取函数
    def parse_param_page(self, response):
        # 获取手机参数字段并一一遍历
        params = response.xpath("//span[contains(@class,'param-name')]")
        for param in params:
            legal_param_name_field = param_name = param.xpath("text()")[0].extract()
            # 将手机参数字段转变为合法的数据库字段（非数字开头，且防止SQL逻辑污染剔除了'/'符号）
            if re.match(r'^\d', param_name):
                legal_param_name_field = re.sub(r'^\d', "f" + param_name[0], param_name)
            if '/' in param_name:
                legal_param_name_field = legal_param_name_field.replace('/', '')
            # 通过查询master表检查动态添加的字段是否已经存在，若不存在则增加该字段
            sql = "SELECT * FROM sqlite_master WHERE name='CELL_PHONES' AND SQL LIKE '%" + legal_param_name_field + "%'"
            if self.database.execute(sql).fetchone() is None:
                sql = "ALTER TABLE CELL_PHONES ADD " + legal_param_name_field + " TEXT"
                self.database.execute(sql)
                self.database.commit()
            # 根据参数字段名的xpath定位参数值元素
            xpath = "//span[contains(@class,'param-name') and text()='" + param_name + \
                    "']/following-sibling::span[contains(@id,'newPmVal')]//text()"
            vals = response.xpath(xpath)
            # 由于有些字段的参数值是多个值，所以需将其附加到一起，合成一个字段，以方便存储。
            # 如需数据细分选用like子句或支持全文索引的数据库也不错，当然nosql更好
            pm_val = ""
            for val in vals:
                pm_val += val.extract()
            re.sub(r'\r|\n', "", pm_val)
            sql = "UPDATE CELL_PHONES SET %s = '%s' WHERE 手机型号 = '%s'" \
                  % (legal_param_name_field, pm_val, response.meta['model'])
            self.database.execute(sql)
            self.counter += 1
            # 检查下爬取的数据对不对
        results = self.database.execute("SELECT * FROM CELL_PHONES").fetchall()
        # 千万别忘了commit否则持久化数据库可能结果不全
        self.database.commit()
        print(self.database.total_changes, self.counter)  # 对比下数据库的增改情况是否有丢失
        for row in results:
            f = open('zolphone.xml', 'a')
            f.write(self.chinese_print(row) + '\n')  # 其实这里有个小小的编码问题需要解决
            f.close()
            # 最后愉快的用scrapy crawl zol 启动爬虫吧
