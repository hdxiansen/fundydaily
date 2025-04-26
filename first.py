'''
@time:2025年4月16日 16:26:30
@author：何等先森
@function：获取数据
'''
import requests
import os
import time
import datetime
import re
import json
import pymongo
import datasplitmysql

client = pymongo.MongoClient(host="192.168.0.53",port = 27017)
fdata = client['etf']
data1 = fdata['data0318']

conn = datasplitmysql.connectsql()

def get_data_daily(pn):
    uri = "https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery371006708902765319946_1742294424245&fs=b%3AMK0021%2Cb%3AMK0022%2Cb%3AMK0023%2Cb%3AMK0024%2Cb%3AMK0827&fields=f12%2Cf13%2Cf14%2Cf1%2Cf2%2Cf4%2Cf3%2Cf152%2Cf5%2Cf6%2Cf17%2Cf18%2Cf15%2Cf16&fid=f3&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=%7C0%7C0%7C0%7Cweb"

    headers = {
        "user-agent":"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/134.0.0.0 Safari/537.36 Edg/134.0.0.0"
    }

    par = {
        'pn':pn,
        'pz':100, #1111
    }

    wb_data = requests.get(uri,headers=headers,params=par)
    # print(wb_data.text)
    # print(time.time())
    data = re.findall(r'\((.*?)\)',wb_data.text)
    # print(data[0])

    wb_data_split = json.loads(data[0])
    wb_data_split['time'] = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    wb_data_split['date'] = time.strftime("%Y-%m-%d",time.localtime())
    data1.insert_one(wb_data_split)
    return wb_data_split
    #wb_data_split的数据居然变了
    # t1 = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
    # print(t1)
    # print(datetime.datetime.now())
    # print(time.localtime())

    #print(json.dumps(json.loads(data[0]),ensure_ascii=False,indent=2))
def datatomysql(pn):
    datasplit = get_data_daily(pn)
    print(datasplit)
    for j in datasplit['data']['diff']:
        data  = {}
        # data['time'] = time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())
        # data['date'] = time.strftime("%Y-%m-%d",time.localtime())
        # data['最新价'] = j['f2']/1000
        # data['涨跌幅'] = j['f3']/10000
        # data['涨跌额'] = j['f4']/1000
        # data['成交量'] = j['f5']/10000 if not j['f5'] == '-' else 0
        # data['成交额'] = j['f6']/10000 if not j['f6'] == "-" else 0
        # data['代码'] = j['f12']
        # data['名称'] = j['f14']
        # data['最高价'] = j['f15']/1000 if not j['f5'] == '-' else 0
        # data['最低价'] = j['f16']/1000 if not j['f5'] == '-' else 0
        # data['开盘价'] = j['f17']/1000 if not j['f5'] == '-' else 0
        # data['咋收'] = j['f18']/1000
        # print(json.dumps(data,ensure_ascii=False,indent=2))
        #print(len(wb_data_split['data']['diff']))
        data['代码'] = j['f12']
        data['名称'] = j['f14']
        data['最新价'] = j['f2']/1000
        data['涨跌幅'] = j['f3']/100
        data['涨跌额'] = j['f4']/1000
        data['成交量'] = j['f5']/10000 if not j['f5'] == '-' else 0
        data['成交额'] = j['f6']/100000000 if not j['f6'] == "-" else 0
        data['最高价'] = j['f15']/1000 if not j['f5'] == '-' else 0
        data['最低价'] = j['f16']/1000 if not j['f5'] == '-' else 0
        data['开盘价'] = j['f17']/1000 if not j['f5'] == '-' else 0
        data['咋收'] = j['f18']/1000
        data['date'] = datasplit['date']
        print(data)

        # 示例字典数据（字段名与表结构一致）
        # 动态生成占位符和字段顺序
        columns = ', '.join(['id','name','zxj','zdf','zde','cjl','cje','zgj','zdj','kpj','zs','daydate'])
        placeholders = ', '.join(['%s'] * len(data))
        dynamic_sql = f"INSERT INTO etf ({columns}) VALUES ({placeholders})"
        try:
            with conn.cursor() as cursor:
                cursor.execute(dynamic_sql, list(data.values()))
            conn.commit()
            print("字典数据插入成功！")
        except Exception as e:
            conn.rollback()
            print(f"字典插入失败: {e}")


#print(json.dumps(json.loads(data[0]),ensure_ascii=False,indent=2))
for i in range(1,13):
    datatomysql(i)

# get_data_daily(12)