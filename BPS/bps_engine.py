import json
import os
import time
from playwright.sync_api import Playwright, sync_playwright, expect
import s3fs


client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

# tahun = [
#     "2003-2005",
#     "2006-2008",
#     "2009-2011",
#     "2012-2014",
#     "2015-2017",
#     "2018-2020"
# ]

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    urls = [
        "https://www.bps.go.id/id/statistics-table/1/MjIxMSMx/angka-kelahiran-total---total-fertility-rate--tfr--hasil-long-form--lf--sp2020-menurut-provinsi-kabupaten-kota--2020.html",
        "https://www.bps.go.id/id/statistics-table/1/MjIxMCMx/angka-kelahiran-total---total-fertility-rate--tfr--menurut-provinsi--1971-2020.html",
        "https://www.bps.go.id/id/statistics-table/1/MjIyMSMx/angka-kematian-anak--child-mortality-rate-cmr--hasil-long-form-sp2020-menurut-provinsi-kabupaten-kota--2020.html",
        "https://www.bps.go.id/id/statistics-table/1/MjIxNiMx/angka-kematian-bayi-akb--infant-mortality-rate-imr--menurut-provinsi---1971-2020.html",
    ]
    for url in urls:
        page.goto(url, timeout=12000000)
        
        name_json= url.split("/")[-1].split(".")[-2] + '.json'
        name_excel = url.split("/")[-1].split(".")[-2] + '.csv'
        
        print(name_json)
        print(name_excel)
        # for year in tahun:
        #     page.locator(".css-1xc3v61-indicatorContainer").click()
        #     # print(year)
        #     page.get_by_text(year, exact=True).click()
        page.get_by_role("button", name="Unduh").click()
        with page.expect_download() as download_info:
            page.get_by_role("menuitem", name="CSV").click(modifiers=["Alt"])
        download = download_info.value
        file_path = os.path.join(os.getcwd(), f'temp_download2023.xls')
        download.save_as(file_path)

        # Upload the file to S3
        nama_file = f"Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.csv"
        save_path = f"ai-pipeline-statistics/data/data_raw/data statistic/BPS/country/excel/{nama_file}"
        with s3.open(save_path, 'wb') as f:
            f.write(open(file_path, 'rb').read())

        os.remove(file_path)
        
        json_name = f"Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.csv"
        metadata = {
            'link':url,
            'domain':'bps.go.id',
            'tag':[
                'angkat buta aksara',
                'bps'
            ],
            'title':f'Prevalensi Ketidakcukupan Konsumsi Pangan (Persen) 2023',
            'update':'2023',
            'desc':'2023',
            'category':'Statistik Demografi dan Sosial',
            'sub_category':'Pendidikan',
            'path_data_raw':[
                f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/country/excel/Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.csv',
                f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/country/json/Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.json'
            ],
            'file_name':[
                f'Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.csv',
                f'Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.json'  
            ],
            'crawling_time_epoch': int(time.time())
        }
        
        with s3.open(f"ai-pipeline-statistics/data/data_raw/data statistic/BPS/country/json/Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.json", "w") as f:
            json.dump(metadata, f)
        print('==================================================')
        print(f'Berhasil menyimpan Prevalensi_Ketidakcukupan_Konsumsi_Pangan_(Persen)_2023.csv ke S3.')
        print(f'Berhasil menyimpan {json_name} ke S3.')
        print('==================================================')
        
    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
