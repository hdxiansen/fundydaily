'''
@time:2025年4月18日 16:24:56
@author：何等先森
@function：
'''

from pymongo import MongoClient
from datetime import datetime, timedelta
import csv

# client = MongoClient(host="192.168.0.53",port = 27017)
# db = client['etf']
# col = db['data0318']

from pymongo import MongoClient
from datetime import datetime, timedelta
import csv

# 配置参数
MONGODB_URI = "mongodb://192.168.0.53:27017/"
DB_NAME = "etf"
COLLECTION_NAME = "data0318"
CUSTOM_DAYS = [1, 2, 3, 5, 7, 10, 30]  # 自定义对比天数窗口
BASE_FIELDS = [f"f{i}" for i in range(1, 19)]  # f1-f18基础字段
DATE_FORMAT = "%Y-%m-%d"

client = MongoClient(MONGODB_URI)
db = client[DB_NAME]
col = db[COLLECTION_NAME]

def generate_date_windows(target_date):
    """生成自定义日期映射表"""
    base_date = datetime.strptime(target_date, DATE_FORMAT)
    return {
        days: {
            "date": (base_date - timedelta(days=days)).strftime(DATE_FORMAT),
            "fields": {f: None for f in BASE_FIELDS}
        }
        for days in CUSTOM_DAYS
    }

def fetch_full_data(target_date):
    """获取完整数据集"""
    date_list = [target_date] + [
        (datetime.strptime(target_date, DATE_FORMAT) - timedelta(days=d)).strftime(DATE_FORMAT)
        for d in CUSTOM_DAYS
    ]
    
    pipeline = [
        {"$match": {"date": {"$in": date_list}}},
        {"$unwind": "$data.diff"},
        {"$addFields": {"data.diff.parent_date": "$date"}},
        {"$replaceRoot": {"newRoot": "$data.diff"}},
        {"$group": {
            "_id": "$f12",
            "records": {
                "$push": {
                    "date": "$parent_date",
                    **{f: f"${f}" for f in BASE_FIELDS}
                }
            }
        }}
    ]
    
    return {item["_id"]: item["records"] for item in col.aggregate(pipeline)}

def build_comparison_row(f12, records, target_date):
    """构建带历史对比的数据行"""
    # 获取当日数据
    current = next((r for r in records if r["date"] == target_date), None)
    if not current:
        return None
    
    row = {"f12": f12, "date": target_date}
    
    # 添加基础字段
    for field in BASE_FIELDS:
        row[field] = current.get(field)
    
    # 计算历史差值
    date_map = {
        r["date"]: r
        for r in records
        if r["date"] != target_date
    }
    
    base_date = datetime.strptime(target_date, DATE_FORMAT)
    for days in CUSTOM_DAYS:
        hist_date = (base_date - timedelta(days=days)).strftime(DATE_FORMAT)
        hist_data = date_map.get(hist_date, {})
        
        current_f3 = current.get("f3")
        hist_f3 = hist_data.get("f3")
        
        delta = current_f3 - hist_f3 if None not in (current_f3, hist_f3) else None
        row[f"f3_prev_{days}d_delta"] = delta
    
    return row

def export_full_dataset(target_date, filename):
    """执行数据导出"""
    full_data = fetch_full_data(target_date)
    
    # 生成CSV头
    headers = ["f12", "date"] + BASE_FIELDS
    headers += [f"f3_prev_{d}d_delta" for d in CUSTOM_DAYS]
    
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        
        count = 0
        for f12, records in full_data.items():
            row = build_comparison_row(f12, records, target_date)
            if row:
                # 清理None值为空字符串
                cleaned_row = {
                    k: v if v is not None else "" 
                    for k, v in row.items()
                }
                writer.writerow(cleaned_row)
                count += 1
                
        print(f"成功导出{count}条记录")

# 使用示例
if __name__ == "__main__":
    # 获取最新日期（示例）
    latest_date = col.find_one(
        {"date": {"$exists": True}},
        sort=[("date", -1)]
    )["date"]
    print(latest_date)
    export_full_dataset(latest_date, "full_analysis_v2.csv")