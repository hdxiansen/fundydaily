'''
@time：2025年4月28日 18:40:05
@author：何等先森
@function：查询语句
'''
from datasplitmysql import connectsql
import os
import time

'''
WITH 
-- 步骤1：计算每个 id 的 zdf 总和
id_total_zdf AS (
    SELECT 
        id, 
        SUM(zdf) AS twoc 
    FROM 
        etf 
    GROUP BY 
        id
),
-- 步骤2：标记每个 id 的最新日期记录
latest_records AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY id ORDER BY daydate DESC) AS rn
    FROM 
        etf
)
-- 步骤3：将总和关联到最新记录
SELECT
    lr.id,
    lr.name,
    lr.zxj,
    lr.zdf,
    lr.zde,
    lr.cjl,
    lr.cje,
    lr.zgj,
    lr.zdj,
    lr.kpj,
    lr.zs,
    lr.daydate,
    t.twoc  -- 添加总累计值
FROM
    latest_records lr
LEFT JOIN
    id_total_zdf t ON lr.id = t.id
WHERE 
    lr.rn = 1;  -- 只保留每个 id 的最新记录
'''

with
id_total_zdf as (
    select id,
    sum(zdf) as twoc
    from etf
    where daydate between '2025-04-25' and '2025-04-28'
    group by id
),
select 
    t.id,
    idt.twoc 
 from etf t
left join
   id_total_zdf idt on t.id = idt.id
where 
   t.daydate = '2025-04-28';
