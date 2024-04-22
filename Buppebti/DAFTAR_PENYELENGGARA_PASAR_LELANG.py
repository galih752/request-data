from datetime import datetime
import json
import os
from bs4 import BeautifulSoup
import requests

url = 'https://bappebti.go.id/pasar_lelang/daftar_penyelenggara'
response = requests.get(url)
soup = BeautifulSoup(response.text, 'html.parser')

# Lakukan penguraian data HTML di sini
# Contoh:
tr_elements = soup.find_all('tr')

for tr in tr_elements[1:]:  # Mulai dari indeks 1 karena indeks 0 adalah header
    td_values = tr.find_all('td')
    
    # Pastikan setiap baris memiliki setidaknya 3 kolom
    if len(td_values) >= 3:
        nama_json = td_values[0].get_text(strip=True).replace(' ', '_') + '.json'
        
        # Memeriksa dan menggantikan nilai kosong dengan None
        penyelenggara = td_values[0].get_text(strip=True) if td_values[0].get_text(strip=True) else None
        alamat = td_values[1].get_text(strip=True) if td_values[1].get_text(strip=True) else None
        ijin = td_values[2].get_text(strip=True) if td_values[2].get_text(strip=True) else None
        
        metadata = {
            "link": 'https://bappebti.go.id/pasar_lelang/daftar_penyelenggara',
            "domain": "bappebti.go.id",
            "tag": [
                "penyelenggara",
                "pasar",
                "lelang",
            ],
            "nama": penyelenggara,
            "alamat": alamat,
            "ijin": ijin,
            "TglIjin": None,
            "path_data_raw": f"s3://ai-pipeline-statistics/data/data_raw/Divtik/Bappebti/penyelenggara_lelang/json/{nama_json}",
            "path_data_clean": f"s3://ai-pipeline-statistics/data/data_clean/Divtik/Bappebti/penyelenggara_lelang/json/{nama_json}",
            "crawling_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "crawling_time_epoch": datetime.now().timestamp()
        }
        
        print(metadata)
        
        os.makedirs('penyelengara_lelang/json', exist_ok=True)
        with open(f'penyelengara_lelang/json/{nama_json}', 'w', encoding='utf-8') as f:
            json.dump(metadata, f, ensure_ascii=False, indent=4)
    else:
        print("Jumlah kolom tidak mencukupi dalam baris ini. Melewatkannya.")
