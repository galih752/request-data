from datetime import datetime
import json
import locale
import time
import requests
import s3fs
from playwright.sync_api import Playwright, sync_playwright, expect

client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

mapping_name = {
    "jabar":"Jawa Barat",
    "riau":"Riau",
    "lampung":"Lampung"
}

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    urls = [
    "https://www.bps.go.id/id/statistics-table/1/MjIxMSMx/angka-kelahiran-total---total-fertility-rate--tfr--hasil-long-form--lf--sp2020-menurut-provinsi-kabupaten-kota--2020.html",
    "https://www.bps.go.id/id/statistics-table/1/MjIxMCMx/angka-kelahiran-total---total-fertility-rate--tfr--menurut-provinsi--1971-2020.html",
    "https://www.bps.go.id/id/statistics-table/1/MjIyMSMx/angka-kematian-anak--child-mortality-rate-cmr--hasil-long-form-sp2020-menurut-provinsi-kabupaten-kota--2020.html",
    "https://www.bps.go.id/id/statistics-table/1/MjIxNiMx/angka-kematian-bayi-akb--infant-mortality-rate-imr--menurut-provinsi---1971-2020.html",
    "https://malut.bps.go.id/indicator/6/251/1/tingkat-partisipasi-angkatan-kerja-tpak-menurut-kabupaten-kota-persen-.html"
]
    for url in urls:
        page.goto(url)
        
        link_awal = url.split('/')[2]
        page.get_by_role("dialog").locator("svg").click()
        nama = page.query_selector('//html/body/div[2]/div[2]/div[1]/h1')
        nama_dua = nama.inner_text().replace(" ", "_")
        nama_json = nama.inner_text().replace(" ", "_") + '.json'
        
        
        tanggal = page.query_selector('//html/body/div[2]/div[2]/div[1]/div[2]/div[2]/table/tbody/tr[4]/td[3]')
        tanggal_dua = tanggal.inner_text()
        
        month_mapping = {
            "Januari": "January",
            "Februari": "February",
            "Maret": "March",
            "April": "April",
            "Mei": "May",
            "Juni": "June",
            "Juli": "July",
            "Agustus": "August",
            "September": "September",
            "Oktober": "October",
            "November": "November",
            "Desember": "December"
        }
        
        for indonesian_month, english_month in month_mapping.items():
            tanggal_dua = tanggal_dua.replace(indonesian_month, english_month)

        # Parse the date string using datetime.strptime
        date_object = datetime.strptime(tanggal_dua, "%d %B %Y")
        # Format the date object as "YY-mm-dd"
        formatted_date = date_object.strftime("%Y-%m-%d")
        
        unduhan = page.query_selector('//html/body/div[2]/div[2]/div[1]/div[2]/div[1]/a')
        unduh_link_asli = unduhan.get_attribute("href")
        response = requests.get(unduh_link_asli)

        path_pdf = f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/pdf/{nama_dua}.pdf"
        # Pastikan permintaan berhasil
        if response.status_code == 200:
            # Tulis konten yang diterima ke file PDF lokal
            try:
                with s3.open(path_pdf, 'wb') as s3_file:
                    s3_file.write(response.content)
                print("Unduhan PDF berhasil dan telah diunggah ke S3.")
                time.sleep(10)
            except Exception as e:
                print(f"Error: {e}")
        
        metadata = {
                "link":  url,
                "tag":  [
                    link_awal,
                    "statistic"
                ],
                "source":  link_awal,
                "location_level":  "province",
                "location":  mapping_name[link_awal.split('.')[0]],
                "title":  None,
                "data_name":  None,
                "range_data":  nama_dua.split("_")[-1],
                "update":  formatted_date,
                "desc":  "Data statistik",
                "category":  None,
                "sub_category":  None,
                "path_data_raw":  [
                    f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/pdf/{nama_dua}.pdf",
                    f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{nama_json}",
                ],
                "file_name": None,
                "crawling_time_epoch":  int(time.time()),
                "file_path":  None,
                "connection_name":  None,
                "dataset_name":  None,
                "file_path_metadata":  f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{nama_json}"
                }
            
        json_s3 = f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{nama_json}"
        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
        with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
            s3_file.write(json_data.encode('utf-8'))
        print('==================================================')
        print(f'File {nama_json} berhasil diupload ke S3.')
        print('==================================================')
            
        print(f'Berhasil menyimpan data {nama_json} ke s3.')
        
    else:
        print("Gagal mengunduh PDF.")
        

    # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
