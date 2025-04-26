'''
@time:2025年4月23日 17:25:03
@author：何等先森
@function：mysql更新数据
'''
#安装mysql-connector-python==8.0.23低版本的才可以，更版本不可用
import mysql.connector

def connectsql():
    try:
        conn = mysql.connector.connect(
            host='localhost',      # 例如 'localhost' 或 '127.0.0.1' 或 'remote_host'
            user='root',
            password='123456',
            database='fund'
        )
        print("连接mysql成功")
        return conn
    except mysql.connector.Error as err:
        print(f"Error: {err}")
    except Exception as e:
        print(e)

def createtale():
    conn = connectsql()
    cursor = conn.cursor()
    print("Connection successful")
    #cjl BIGINT NOT NULL COMMENT \'成交量\',\
    sql_one = "CREATE TABLE if not exists etf (\
        num INT AUTO_INCREMENT PRIMARY KEY,\
        id VARCHAR(50) NOT NULL COMMENT \'代码\',\
        name VARCHAR(255) NOT NULL COMMENT \'ETF名称\',\
        zxj DECIMAL(10, 3) NOT NULL COMMENT \'最新价\',\
        zdf DECIMAL(5, 2) NOT NULL COMMENT \'涨跌幅\',\
        zde DECIMAL(10, 3) NOT NULL COMMENT \'涨跌额\',\
        cjl DECIMAL(15, 2) NOT NULL COMMENT \'成交额\',\
        cje DECIMAL(15, 2) NOT NULL COMMENT \'成交额\',\
        zgj DECIMAL(10, 3) NOT NULL COMMENT \'最高价\',\
        zdj DECIMAL(10, 3) NOT NULL COMMENT \'最低价\',\
        kpj DECIMAL(10, 3) NOT NULL COMMENT \'开盘价\',\
        zs DECIMAL(10, 3) NOT NULL COMMENT \'昨收价\',\
        daydate DATE  COMMENT \'交易日期\'\
    ) COMMENT \'ETF每日交易数据表\';"
    cursor.execute(sql_one)
    insert_sql = 'INSERT INTO etf (id, name, zxj, zdf, zde, cjl, cje, zgj, zdj, kpj, zs, daydate) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'
    # cursor.execute(insert_sql, ('560560','碳中和ETF泰康',0.517,0.058,0.003,10.5215,544.9092,0.519,0.515,0.515,0.514,'2025-04-24'))
    conn.commit()
    
    for i in cursor:
        print(i)

createtale()

# # 示例字典数据（字段名与表结构一致）
# dict_data = {
#     'id': '510300',
#     'name': '沪深300ETF',
#     'zxj': 3.80,
#     'zdf': 0.53,
#     'zde': 0.02,
#     'cjl': 1000000,
#     'cje': 3800000.00,
#     'zgj': 3.82,
#     'zdj': 3.78,
#     'kpj': 3.79,
#     'zs': 3.78,
#     'daydate': '2023-10-01'
# }

# # 动态生成占位符和字段顺序
# columns = ', '.join(dict_data.keys())
# placeholders = ', '.join(['%s'] * len(dict_data))

# print(placeholders)
# dynamic_sql = f"INSERT INTO etf ({columns}) VALUES ({placeholders})"

# try:
#     with connection.cursor() as cursor:
#         cursor.execute(dynamic_sql, list(dict_data.values()))
#     connection.commit()
#     print("字典数据插入成功！")
# except Exception as e:
#     connection.rollback()
#     print(f"字典插入失败: {e}")