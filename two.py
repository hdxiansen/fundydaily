'''
@time:2025年4月16日 16:27:04
@author：何等先森
@function：提取数据
'''
import os
import time
import re
import json
import pymongo
import datetime
from bson import ObjectId
import csv

class MyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            print("MyEncoder-datetime.datetime")
            return obj.strftime("%Y-%m-%d %H:%M:%S")
        if isinstance(obj, bytes):
            return str(obj, encoding='utf-8')
        if isinstance(obj, int):
            return int(obj)
        elif isinstance(obj, float):
            return float(obj)
        elif isinstance(obj, ObjectId):
            return str(obj)
        else:
            return super(MyEncoder, self).default(obj)

client = pymongo.MongoClient(host="192.168.0.53",port = 27017)
fdata = client['etf']
data1 = fdata['data0318']

with open(r'et.csv','w',newline='',encoding='utf-8',)as f:
    csv_writer = csv.writer(f)
    for i in data1.find():
        # print(i)
        # print(type(i))
        #print(i)
        # print(json.dumps(i,cls =MyEncoder,ensure_ascii=False,indent=2))
        #f12 代码
        #f14 名称
        #f2 最新价 /1000
        #f3 涨跌幅 /1000
        #f4 涨跌额 /1000
        #f5 成交量 /1000
        #f6 成交额 /100000000
        #f15 最高价 /1000
        #f16 最低价 /1000
        #f17 开盘价 /1000
        #f18 咋收 /1000
        try:
            for j in i['data']['diff']:
                data  = {}
                data['最新价'] = j['f2']/1000
                data['涨跌幅'] = j['f3']/1000
                data['涨跌额'] = j['f4']/1000
                data['成交量'] = j['f5']/10000 if not j['f5'] == '-' else 0
                data['成交额'] = j['f6']/10000 if not j['f6'] == "-" else 0
                data['代码'] = j['f12']
                data['名称'] = j['f14']
                data['最高价'] = j['f15']/1000 if not j['f5'] == '-' else 0
                data['最低价'] = j['f16']/1000 if not j['f5'] == '-' else 0
                data['开盘价'] = j['f17']/1000 if not j['f5'] == '-' else 0
                data['咋收'] = j['f18']/1000
                data['time'] = i['time']
                data['date'] = i['date']
                #print(data)
                #csv_writer.writerow(data.values())
        except:
            pass
    
    for j in data1.find():
        try:
            if j['date'] == '2025-04-16':
                print(j)
        except:
            pass
