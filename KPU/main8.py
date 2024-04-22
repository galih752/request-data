from datetime import datetime
import json
import time
from urllib.parse import urlparse
from loguru import logger
from playwright.sync_api import sync_playwright
import asyncio
import s3fs
import requests

client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

def kpu():
    provinsi_url = 'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/0.json'
    response = requests.get(provinsi_url)
    response.raise_for_status()
    data_prov = response.json()
    
    kode_jawa = [
        # 32,
        #33,
        # 34,
        # 35
        76,
        81,
        82,
        91,
        92
    ]
    
    provinsi = ''
    for datas_prov in kode_jawa:
        if datas_prov == 76:
            provinsi_name = 'Sulawesi Barat'
            provinsi = provinsi_name.upper()
        elif datas_prov == 81:
            provinsi_name = 'Maluku'
            provinsi = provinsi_name.upper()
        elif datas_prov == 82:
            provinsi_name = 'Maluku Utara'
            provinsi = provinsi_name.upper()
        elif datas_prov == 91:
            provinsi_name = 'Papua'
            provinsi = provinsi_name.upper()
        elif datas_prov == 92:
            provinsi_name = 'Papua Barat'
            provinsi = provinsi_name.upper()
        
        print(provinsi)
        # provinsi = datas_prov['nama']
        # kode = datas_prov['kode']
        # tingkat = datas_prov['tingkat']
        
        kabkot_url = f'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{datas_prov}.json'
        response = requests.get(kabkot_url)
        response.raise_for_status()
        data_kabkot = response.json()
        for datas_kabkot in data_kabkot:
            kab_kota = datas_kabkot['nama']
            kode_kab = datas_kabkot['kode']
            tingkat_kab = datas_kabkot['tingkat']
            kec_url = f'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{datas_prov}/{kode_kab}.json'
            response = requests.get(kec_url)
            response.raise_for_status()
            data = response.json()
            for datas in data:
                kecamatan = datas['nama']
                kode_kecamatan = datas['kode']
                tingkat = datas['tingkat']
                des_url = f'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{datas_prov}/{kode_kab}/{kode_kecamatan}.json'
                response = requests.get(des_url)
                response.raise_for_status()
                data = response.json()
                for datas_des in data:
                    desa = datas_des['nama']
                    kode_desa = datas_des['kode']
                    tingkat = datas_des['tingkat']
                    tps_url = f'https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{datas_prov}/{kode_kab}/{kode_kecamatan}/{kode_desa}.json'
                    response = requests.get(tps_url)
                    response.raise_for_status()
                    data = response.json()
                    for datas_tps in data:
                        tps = datas_tps['nama']
                        kode_tps = datas_tps['kode']
                        tingkat = datas_tps['tingkat']
                        data_url = f'https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{datas_prov}/{kode_kab}/{kode_kecamatan}/{kode_desa}/{kode_tps}.json'
                        logger.info(data_url)
                        response = requests.get(data_url)
                        response.raise_for_status()
                        data = response.json()
                        
                        # print(json.dumps(data, indent=4))
                        path_data_raw = []
                        for idx, image_url in enumerate(data["images"], start=1):
                            if image_url is not None:
                                response = requests.get(image_url)
                                if response.status_code == 200:
                                    # Mendapatkan nama file dari URL gambar
                                    
                                    path_data = f's3://ai-pipeline-statistics/data/data_raw/KPU/Hasil Hitung Suara/image/{provinsi}/{kab_kota}/{kecamatan}/{desa}/{tps}/image({idx}).jpg'
                                    # Menulis data gambar ke S3
                                    with s3.open(path_data, 'wb') as f:
                                        f.write(response.content)
                                    
                                    print(f"Gambar {path_data} berhasil diunggah ke S3.")
                                    path_data_raw.append(path_data)
                                
                        path_json = f's3://ai-pipeline-statistics/data/data_raw/KPU/Hasil Hitung Suara/image/{provinsi}/{kab_kota}/{kecamatan}/{desa}/{tps}/metadata({idx}).json'
                        path_data_raw.append(path_json)
                        metadata = {
                            'link': f'https://pemilu2024.kpu.go.id/pilpres/hitung-suara/{datas_prov}/{kode_kab}/{kode_kecamatan}/{kode_desa}/{kode_tps}',
                            'domain': 'kpu.go.id',
                            'tag': [
                                'kpu',
                                'hasil hitung suara'
                            ],
                            'province': provinsi,
                            'city': kab_kota,
                            'district': kecamatan,
                            'subdistrict': desa,
                            'tps': tps,
                            'path_data_raw': path_data_raw,
                            'crawling_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                            'crawling_time_epoch': int(time.time())
                            
                        }

                        with s3.open(path_json, 'w') as f:
                            f.write(json.dumps(metadata, indent=4))
                            
                        print('====================================================')
                        print(f"Metadata {path_json}.json berhasil diunggah ke S3.")
                        print(metadata)
                        print('====================================================')
                        print(f'Link : {data_url}')
                        print('====================================================')
        
if __name__ == "__main__":
    asyncio.run(kpu())