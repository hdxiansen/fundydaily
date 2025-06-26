'''
@time:2025年6月23日 18点53分
@author：何等先森
@function：查询2天-14天的求和项
'''
import datasplitmysql

conn = datasplitmysql.connectsql()

sum_sql = '''
    WITH
    id_zdf AS (
    SELECT 
        id,
        name,
        zxj,
        zdf,
        zde,
        cjl,
        cje,
        zgj,
        zdj,
        kpj,
        zs,
        daydate,
        -- 2日累计（当日 + 前1日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 1 PRECEDING AND CURRENT ROW
        ) AS 2day_sum,
        -- 3日累计（当日 + 前2日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        ) AS 3day_sum,
        -- 4日累计（当日 + 前3日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
        ) AS 4day_sum,
        -- 5日累计（当日 + 前4日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 4 PRECEDING AND CURRENT ROW
        ) AS 5day_sum,
        -- 7日累计（当日 + 前6日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS 7day_sum,
        -- 14日累计（当日 + 前13日）
        SUM(zdf) OVER (
            PARTITION BY id 
            ORDER BY daydate 
            ROWS BETWEEN 13 PRECEDING AND CURRENT ROW
        ) AS 14day_sum
    FROM 
        etf
    ORDER BY 
        id, daydate
    )
    SELECT
    df.daydate,
    df.id,
    df.name,
    df.zxj,
    df.zdf,
    df.zde,
    df.cjl,
    df.cje,
    df.zgj,
    df.zdj,
    df.kpj,
    df.zs,
    df.2day_sum,
    df.3day_sum,
    df.4day_sum,
    df.5day_sum,
    df.7day_sum,
    df.14day_sum
    from id_zdf df
    where df.daydate = curdate();
'''
cursor = conn.cursor()
cursor.execute(sum_sql)
result = cursor.fetchall()
for row in result:
    print(row)
conn.commit()
print("query success")
