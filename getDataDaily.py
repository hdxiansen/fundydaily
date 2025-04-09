'''
@time:20250407 22:09:00
@author:何等先森
@function:get data about
'''
import requests
import json
import re

uri = 'https://push2.eastmoney.com/api/qt/clist/get?np=1&fltt=1&invt=2&cb=jQuery37108270355613565856_1744034291954&fs=b%3AMK0021%2Cb%3AMK0022%2Cb%3AMK0023%2Cb%3AMK0024%2Cb%3AMK0827&fields=f12%2Cf13%2Cf14%2Cf1%2Cf2%2Cf4%2Cf3%2Cf152%2Cf5%2Cf6%2Cf17%2Cf18%2Cf15%2Cf16&fid=f3&pn=1&pz=1200&po=1&dect=1&ut=fa5fd1943c7b386f172d6893dbfba10b&wbp2u=%7C0%7C0%7C0%7Cweb&_=1744034291970'

headers = {
    'Accept':'*/*',
    #Accept-Encoding: gzip, deflate, br
    #Accept-Language: zh-CN,zh;q=0.9,en;q=0.8,en-GB;q=0.7,en-US;q=0.6
    #Connection: keep-alive
    #Cookie: qgqp_b_id=637479827eba36e066428ba14470eff7; websitepoptg_api_time=1744034250291; st_si=44831611505518; st_asi=delete; fullscreengg=1; fullscreengg2=1; st_pvi=56098555283796; st_sp=2025-04-07%2021%3A57%3A30; st_inirUrl=https%3A%2F%2Fwww.baidu.com%2Flink; st_sn=4; st_psi=20250407215819499-113200301321-0137418638
    #Host: push2.eastmoney.com
    #Referer: https://quote.eastmoney.com/center/gridlist.html
    #sec-ch-ua: " Not A;Brand";v="99", "Chromium";v="100", "Microsoft Edge";v="100"
    #sec-ch-ua-mobile: ?0
    #sec-ch-ua-platform: "Windows"
    #Sec-Fetch-Dest: script
    #Sec-Fetch-Mode: no-cors
    #Sec-Fetch-Site: same-site
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36 Edg/100.0.1185.36',
}

data = requests.get(uri,headers=headers)
print(data.text)
print(type(data.text))
split_data = re.findall(r'\((.*?)\)',data.text)[0]
print(json.dumps(json.loads(split_data),indent=2,ensure_ascii=False))