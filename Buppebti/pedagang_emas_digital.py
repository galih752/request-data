from datetime import datetime
import json
import os
import requests

cookies = {
    'ci_session': '74337cda35f756a5641f12824aa1ddc21d3f121f',
    '_gid': 'GA1.3.769437555.1708921092',
    '_gat_gtag_UA_111717980_1': '1',
    '_ga': 'GA1.1.704592022.1708921092',
    '_ga_RWMXHZCX20': 'GS1.1.1708921092.1.1.1708921167.0.0.0',
}

headers = {
    'Accept': 'application/json, text/javascript, */*; q=0.01',
    'Accept-Language': 'en-US,en;q=0.9',
    'Connection': 'keep-alive',
    'Content-Type': 'application/json',
    # 'Cookie': 'ci_session=74337cda35f756a5641f12824aa1ddc21d3f121f; _gid=GA1.3.769437555.1708921092; _gat_gtag_UA_111717980_1=1; _ga=GA1.1.704592022.1708921092; _ga_RWMXHZCX20=GS1.1.1708921092.1.1.1708921167.0.0.0',
    'Referer': 'https://bappebti.go.id/pedagang_emas',
    'Sec-Fetch-Dest': 'empty',
    'Sec-Fetch-Mode': 'cors',
    'Sec-Fetch-Site': 'same-origin',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'X-Requested-With': 'XMLHttpRequest',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
}

params = {
    'sort': 'Nama',
    'order': 'asc',
    'offset': '0',
    'limit': '10',
}

response = requests.get(
    'https://bappebti.go.id/perantara_emas/list_perantara_emas?sort=Nama&order=asc&offset=0&limit=10',
    params=params,
    cookies=cookies,
    headers=headers,
)

data = response.json()
for datas in data:
    
    nama_json = datas['Nama'].replace(' ','_') + '.json'
    
    metadata = {
        "link":datas['website'],
        "domain": "indogold.id",
        "tag":[
            "pedagang",
            "emas",
            "digital",
        ],
        "nama": datas['Nama'],
        "alamat": datas['Alamat'],
        "Ijin": datas['Ijin'],
        "TglIjin": datas['TglIjin'],
        "path_data_raw": f"s3://ai-pipeline-statistics/data/data_raw/Divtik/Bappebti/Perantara Pedagang Fisik Emas Digital/json/{nama_json}",
        "path_data_clean": f"s3://ai-pipeline-statistics/data/data_clean/Divtik/Bappebti/Perantara Pedagang Fisik Emas Digital/json/{nama_json}",
        "crawling_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "crawling_time_epoch": datetime.now().timestamp()
        }
    
    os.makedirs('Perantara Pedagang Fisik Emas Digital/json', exist_ok=True)
    with open(f'Perantara Pedagang Fisik Emas Digital/json/{nama_json}', 'w', encoding='utf-8') as f:
        json.dump(metadata, f, ensure_ascii=False, indent=4)
    
    
    print(metadata)