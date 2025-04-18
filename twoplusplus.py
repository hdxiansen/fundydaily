'''
@time:2025年4月18日 16:24:56
@author：何等先森
@function：
'''

from pymongo import MongoClient
from datetime import datetime, timedelta

client = MongoClient(host="192.168.0.53",port = 27017)
db = client['etf']
col = db['data0318']

def get_time_windows(today_date, max_days=30):
    """生成需要比较的时间窗口"""
    base_date = datetime.strptime(today_date, "%Y-%m-%d")
    return {
        f"prev_{n}d": (base_date - timedelta(days=n)).strftime("%Y-%m-%d")
        for n in range(1, max_days+1)
    }

def fetch_comparison_data(target_date, lookback_days=30):
    # 生成日期范围
    date_ranges = get_time_windows(target_date, lookback_days)
    all_dates = [target_date] + list(date_ranges.values())
    
    # 聚合查询（添加 f12 存在性检查）
    pipeline = [
        {"$match": {"date": {"$in": all_dates}}},
        {"$unwind": "$data.diff"},
        {"$match": {"data.diff.f12": {"$exists": True}}},  # 确保 f12 存在
        {"$addFields": {
            "data.diff.date": "$date",
            "data.diff.timestamp": {"$toDate": "$date"}
        }},
        {"$replaceRoot": {"newRoot": "$data.diff"}},
        {"$sort": {"timestamp": -1}},
        {"$group": {
            "_id": "$f12",
            "history": {"$push": "$$ROOT"}
        }},
        {"$project": {
            "f12": "$_id",
            "history": 1,
            "_id": 0
        }}
    ]
    
    return list(col.aggregate(pipeline))

def build_comparison(target_date):
    results = fetch_comparison_data(target_date, 30)
    final_output = []
    
    for item in results:
        # 健壮性检查：确保包含 history 字段
        if 'history' not in item or not item['history']:
            print(f"警告: 跳过无效条目 {item.get('f12', '未知')}")
            continue
        
        # 按时间排序历史记录
        sorted_history = sorted(
            item['history'], 
            key=lambda x: x['timestamp'], 
            reverse=True
        )
        
        # 提取当天数据
        today_data = next(
            (d for d in sorted_history if d['date'] == target_date),
            None
        )
        
        if not today_data:
            continue
        
        # 构建比较结构
        comparison = {
            "metadata": {
                "f12": item['f12'],
                "f14": today_data.get('f14'),
                "current_date": target_date
            },
            "current_data": {k: v for k, v in today_data.items() if k.startswith('f')},
            "comparisons": {}
        }
        
        # 生成历史比较
        for n in range(1, 31):
            prev_date = (datetime.strptime(target_date, "%Y-%m-%d") - timedelta(days=n)).strftime("%Y-%m-%d")
            prev_data = next(
                (d for d in sorted_history if d['date'] == prev_date),
                None
            )
            
            comparison['comparisons'][f"prev_{n}d"] = {
                "date": prev_date,
                "exists": bool(prev_data),
                "f3": prev_data['f3'] if prev_data else None,
                "f3_delta": today_data['f3'] - prev_data['f3'] if prev_data else None,
                "f3_pct_change": (
                    (today_data['f3'] - prev_data['f3']) / prev_data['f3'] * 100 
                    if prev_data and prev_data['f3'] != 0 else None
                ),
                "full_data": prev_data if prev_data else None
            }
        
        final_output.append(comparison)
    
    return final_output

# 使用示例 ---------------------------------------------------
if __name__ == "__main__":
    # 自动获取最新日期（或手动指定）
    latest_date = col.find_one(
        sort=[("date", -1)],
        projection={"date": 1}
    )['date']
    
    analysis_results = build_comparison(latest_date)
    
    # 打印结果
    for result in analysis_results:
        print(f"\n产品代码: {result['metadata']['f12']}")
        print(f"当前日期: {result['metadata']['current_date']}")
        print("今日数据:")
        for key in sorted(result['current_data'].keys()):
            print(f"  {key}: {result['current_data'][key]}")
        
        print("\n历史比较:")
        for days in ['prev_1d', 'prev_7d', 'prev_30d']:  # 示例展示关键日期
            comp = result['comparisons'].get(days)
            if comp:
                print(f"{days}:")
                print(f"  日期: {comp['date']} | 数据存在: {'是' if comp['exists'] else '否'}")
                print(f"  f3值: {comp['f3'] or 'N/A'}")
                print(f"  变化量: {comp['f3_delta'] or 'N/A'}")
                print(f"  变化率: {comp['f3_pct_change'] or 'N/A'}\n")