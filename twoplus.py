'''
@time:2025年4月17日 19:00:19
@author：何等先森
@function：过滤数据
'''

from pymongo import MongoClient
from datetime import datetime
import json
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
# 连接 MongoDB

client = MongoClient(host="192.168.0.53",port = 27017)
db = client['etf']  # 替换为实际数据库名
collection = db['data0318']  # 替换为实际集合名

def compare_f3_values(dates,start_date,end_date):
    # 聚合管道
    pipeline = [
        {"$match": {"date": {"$in": dates}}},  # 筛选指定日期
        {"$unwind": "$data.diff"},  # 展开 diff 数组
        {"$addFields": {"data.diff.date": "$date"}},  # 添加日期字段
        {"$replaceRoot": {"newRoot": "$data.diff"}},  # 提升 diff 为根文档
        {"$group": {  # 按 f12 分组
            "_id": "$f12",
            "entries": {
                "$push": {
                    "date": "$date",
                    "data": "$$ROOT"
                }
            }
        }},
        {"$project": {  # 整理输出格式
            "_id": 0,
            "f12": "$_id",
            "entries": 1
        }}
    ]
    # 按时间范围查询
 
    pipeline[0] = {"$match": {"date": {"$gte": start_date, "$lte": end_date}}}

    # 执行查询
    results = list(collection.aggregate(pipeline))
    
    # 处理结果
    comparison_data = []
    for item in results:
        f12 = item['f12']
        entries = sorted(item['entries'], key=lambda x: x['date'])  # 按日期排序
        
        # 存储所有字段数据
        full_data = {entry['date']: entry['data'] for entry in entries}
        
        # 比较 f3 值
        if len(entries) >= 2:
            f3_values = {entry['date']: entry['data']['f3'] for entry in entries}
            comparison = {
                "f12": f12,
                "f3_comparison": f3_values,
                "full_data": full_data
            }
            comparison_data.append(comparison)

        # 在比较结果中添加变化标识
        if len(f3_values) == 2:
            values = list(f3_values.values())
            change = values[1] - values[0]
            comparison["f3_change"] = {
                "absolute": change,
                "percentage": f"{(change/values[0])*100:.2f}%"
            }
    return comparison_data

def export_to_csv(data, filename):
    with open(filename, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow(['产品代码', '日期', 'f3值'] + [f'f{i}' for i in range(1, 153)])
        
        for item in data:
            for date, values in item['full_data'].items():
                row = [item['f12'],date,values.get('f3', 'N/A')] + [values.get(f'f{i}', '') for i in range(1, 153)]
                writer.writerow(row)


if __name__ == "__main__":
    # 要比较的日期列表
    # 按时间范围查询
    start_date = "2025-04-01"
    end_date = "2025-04-30"

    target_dates = ["2025-04-18", "2025-04-16",]
    
    # 获取比较结果
    results = compare_f3_values(target_dates,start_date,end_date)
    # print(json.dumps(results,indent=2,ensure_ascii=False))
    #打印输出
    for result in results:
        print(f"\n产品代码: {result['f12']}")
        print("f3 值比较:")
        for date, value in result['f3_comparison'].items():
            print(f"  {date}: {value}")
        
        print("\n完整字段数据:")
        for date, data in result['full_data'].items():
            print(f"\n日期 {date}:")
            for key in sorted(data.keys()):
                if key.startswith('f') and key[1:].isdigit():
                    print(f"  {key}: {data[key]}")
    # export_to_csv(results,'22.csv')