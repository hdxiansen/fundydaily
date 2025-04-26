'''
@time:2025年4月19日 10:44:12
@author：何等先森
@function：文心一言的gx逻辑
'''
import datetime
from pymongo import MongoClient

# 替换为你的MongoDB连接字符串、数据库名称和集合名称
client = MongoClient('host="192.168.0.53",port = 27017')
db = client['etf']
collection = db['data0318']

# 1. 提取时间数据并找到最新日期
print(list(collection.find().sort('time', -1).limit(1)))
documents = list(collection.find().sort('time', -1).limit(1))
latest_date_str = documents[0]['date'] if documents else None

# 将字符串日期转换为datetime对象以便计算
latest_date = datetime.datetime.strptime(latest_date_str, '%Y-%m-%d') if latest_date_str else None

# 2. 计算f3字段在不同时间间隔内的总和
intervals = {
    '前1天': 1,
    '前2天': 2,
    '前3天': 3,
    '前4天': 4,
    '前5天': 5,
    '前7天': 7,
    '前10天': 10,
    '前30天': 30
}

sums = {}
for interval_name, interval_days in intervals.items():
    if latest_date and interval_days > 0:
        start_date = latest_date - datetime.timedelta(days=interval_days)
        # 查询该时间间隔内的文档
        query = {
            'date': {
                '$gte': start_date.strftime('%Y-%m-%d'),
                '$lt': latest_date.strftime('%Y-%m-%d')
            }
        }
        cursor = collection.find(query)
        total_sum = sum(doc['data']['diff'][0]['f3'] for doc in cursor)  # 假设每个文档的diff列表至少有一个元素
        sums[interval_name] = total_sum

# 3. 输出最新日期的完整数据
latest_data = documents[0]['data']['diff'] if documents else []

# 打印结果
# print("最新日期:", latest_date_str)
# print("f3字段在不同时间间隔内的总和:")
# for interval_name, total_sum in sums.items():
#     print(f"{interval_name}: {total_sum}")
# print("最新日期的完整数据:")
# for item in latest_data:
#     print(item)

time.strftime("%Y-%m-%d %H:%M:%S",time.localtime())