import asyncio
import json
import os
import re
import time
from loguru import logger
from playwright.async_api import async_playwright

MAX_FILENAME_LENGTH = 100

def truncate_filename(name):
    if len(name) > MAX_FILENAME_LENGTH:
        extension = name[name.rfind('.'):]  # Dapatkan ekstensi file
        truncated_name = name[:MAX_FILENAME_LENGTH - len(extension) - 1] + extension  # Potong nama sesuai panjang maksimum
        return truncated_name
    return name
category = "Sosial dan Kependudukan"
sub_category = "Agama"

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        contex = await browser.new_context()
        page = await contex.new_page()
        urls = [
            # "https://okuselatankab.bps.go.id/indicator/28/193/1/angka-melek-huruf-menurut-jenis-kelamin-dan-kabupaten-kota-di-provinsi-sumatera-selatan.html"
            # "https://sumsel.bps.go.id/indicator/104/860/1/produk-domestik-regional-bruto-per-kapita-atas-dasar-harga-berlaku-menurut-kabupaten-kota.html"
            # "https://jateng.bps.go.id/indicator/157/1743/1/-seri-2010-laju-pertumbuhan-pdrb-atas-dasar-harga-konstan-2010-menurut-kabupaten-kota-di-provinsi-jawa-tengah.html",
            # "https://jateng.bps.go.id/indicator/157/1746/1/-seri-2010-pdrb-per-kapita-atas-dasar-harga-berlaku-menurut-kabupaten-kota-di-provinsi-jawa-tengah.html",
            # "https://jateng.bps.go.id/indicator/157/1740/1/-seri-2010-pdrb-atas-dasar-harga-berlaku-menurut-kabupaten-kota-di-provinsi-jawa-tengah.html"
            # "https://sulbar.bps.go.id/indicator/33/511/1/-seri-2010-produk-domestik-regional-bruto-per-kapita-atas-dasar-harga-konstan.html",
            # "https://sulbar.bps.go.id/indicator/33/462/1/-seri-2010-laju-pertumbuhan-produk-domestik-regional-bruto-atas-dasar-harga-konstan-2010.html"
            # "https://kepri.bps.go.id/indicator/52/642/1/-seri-2010-pdrb-perkapita-kabupaten-kota-atas-dasar-harga-konstan.html"
            # "https://sumsel.bps.go.id/indicator/23/604/1/persentase-penduduk-miskin-menurut-kabupaten-kota.html"
            # "https://jateng.bps.go.id/indicator/23/34/1/kemiskinan.html",
            # "https://ntb.bps.go.id/indicator/23/460/1/persentase-penduduk-miskin-provinsi-nusa-tenggara-barat-menurut-kabupaten-kota.html"
            # "https://bangkakab.bps.go.id/indicator/12/253/1/jumlah-penduduk-menurut-kelompok-umur-dan-jenis-kelamin.html",
            # "https://jateng.bps.go.id/indicator/5/143/1/rata-rata-pengeluaran-per-kapita-per-bulan-makanan-dan-bukan-makanan.html"
            "https://gorontalo.bps.go.id/indicator/108/78/1/persentase-penduduk-menurut-agama.html"
        ]
        
        
        for url in urls:
            await page.goto(url, timeout=12000000)
            
            awalan_link = url.split("/")[2].split(".")[0]
            
            nama_file = url.split("/")[-1].split(".")[-2]
            
            province_name =  url.split('.')[-5].split('/')[2]
            logger.debug(province_name)
            
            try:
                popup = await page.query_selector('a[title="Close"]')
                await popup.click()
            except Exception as e:
                logger.info("no popup")
            
            ranges_item = await page.query_selector_all('ul#yw0 li a')
            if ranges_item:
                try:
                    for ranges in ranges_item:
                        range_text = await ranges.inner_text()
                        range_text = range_text.replace('\n',' ')
                        link_range = await ranges.get_attribute('href')
                    
                        range_link = ''
                        if f'https://{awalan_link}.bps.go.id' in link_range:
                            range_link += link_range
                        else:
                            range_link += f"https://{awalan_link}.bps.go.id{link_range}"
                        
                        logger.success(range_link)
                        page2 = await contex.new_page()
                        await page2.goto(f"{range_link}", timeout=120000)
                        
                        title = await page2.query_selector('//*[@id="column2"]/h4')
                        title_text = await title.inner_text()
                        logger.success(title_text)
                        
                        data_name = f'{truncate_filename(title_text)}'
                        
                        pdf_satu = await page2.query_selector('//*[@id="column2"]/div[1]/button')
                        try:
                            if pdf_satu:
                                async with page2.expect_download() as download_info:
                                        await page2.get_by_role("button", name=" xlsx").click()
                                download = await download_info.value
                            
                                nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', data_name)}({range_text}).xlsx"
                                nama_file = nama_file
                                save_path = f"province/{province_name.title()}/{category}/{sub_category}/file_excel/{nama_file}"
                                
                                await download.save_as(save_path)
                                
                                
                                file_name_json = f'{re.sub(r"[^a-zA-Z0-9]+", "_", data_name)}({range_text}).json'
                                file_name_json = file_name_json
                                json_file_name = f"province/{province_name.title()}/{category}/{sub_category}/file_json/{file_name_json}"
                                current_time = int(time.time())

                                data = {
                                    "link": f"{url}",
                                    "tag": [
                                        "bps",
                                        province_name,
                                        "tabulardata"
                                    ],
                                    "source": f"{province_name.lower()}.bps.go.id",
                                    "location_level": "province",
                                    "location": province_name,
                                    "title": f'{title_text}',
                                    "data_name": f'{data_name}',
                                    "range_data": range_text,
                                    "update": range_text,
                                    "desc": range_text,
                                    "category": category,
                                    "sub_category": sub_category,
                                    "path_data_raw": [
                                        f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/{save_path}',
                                        f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/{json_file_name}'
                                    ],
                                    "file_name": [
                                        f'{nama_file}',
                                        f'{file_name_json}'
                                    ],
                                    "crawling_time_epoch": current_time
                                }
                                    
                                if not os.path.exists(f'province/{province_name}/{category}/{sub_category}/file_json'):
                                    os.makedirs(f'province/{province_name}/{category}/{sub_category}/file_json', exist_ok=True)
                                
                                if not os.path.exists(f'province/{province_name}/{category}/{sub_category}/file_excel'):
                                    os.makedirs(f'province/{province_name}/{category}/{sub_category}/file_excel', exist_ok=True)

                                with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                        except Exception as e:
                            logger.error (e)
                except Exception as e:
                    logger.error (e)
        await page.close()

if __name__ == '__main__':
    asyncio.run(main())