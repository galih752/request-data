import requests
import json
from datetime import datetime
import re
import s3fs
import os
import time

# client_kwargs = {
#     'key': '63CTHXWC618V2KYFMBK8',
#     'secret': 'NJrKGgTDEMsf3GAFpYLBMa9RhARnfHIaSvr8Wu2b',
#     'endpoint_url': 'http://192.168.180.9:8000',
#     'anon': False
# }
# s3 = s3fs.core.S3FileSystem(**client_kwargs)

# excel_formats = ['xlsx', 'xls']
for i in range(0, 3):
    url = f"https://katalog.data.go.id/organization/kementerian-dalam-negeri?organization=kementerian-dalam-negeri&res_format=JSON&page={i}"

    response = requests.get(url)

    if response.status_code == 200:
        data = response.json()
        results = data.get("result", {}).get("results", [])

        current_time = int(time.time())
        formatted_time = datetime.utcfromtimestamp(current_time).strftime('%Y-%m-%d 00:00:00')
        for item in results:
            extras = item.get("extras", [])
            jadwal_pemutakhiran = ''
            category_data = ''

            # Cari nilai untuk kunci "Jadwal Pemutakhiran Data"
            for extra in extras:
                value = extra.get("value")
                if extra.get("key") == "Jadwal Pemutakhiran Data" or extra.get("key") == "Frekuensi Penyajian":
                    jadwal_pemutakhiran += value if value is not None else ''
                elif extra.get("key") == "kategori":
                    category_data += value if value is not None else ''
                elif extra.get("key") == "harvest_object_id" or extra.get("key") == "harvest_source_id" or extra.get("key") == "harvest_source_title":
                    # Mengatasi kunci-kunci ini dengan mengubah nilai None menjadi string kosong
                    category_data += '' if value is None else ''
                    jadwal_pemutakhiran += '' if value is None else ''