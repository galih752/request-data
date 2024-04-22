from playwright.async_api import async_playwright
import asyncio
import time
from loguru import logger
import json
import re
import os

MAX_FILENAME_LENGTH = 100

def truncate_filename(name):
    if len(name) > MAX_FILENAME_LENGTH:
        extension = name[name.rfind('.'):]  # Dapatkan ekstensi file
        truncated_name = name[:MAX_FILENAME_LENGTH - len(extension) - 1] + extension  # Potong nama sesuai panjang maksimum
        return truncated_name
    return name

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=False)
        contex = await browser.new_context()
        page = await contex.new_page()
        urls = [
            # "https://papuabarat.bps.go.id/indicator/6/270/1/tingkat-pengangguran-terbuka-tpt-menurut-kabupaten-kota.html"
            # "https://papuabarat.bps.go.id/statictable/2018/10/24/201/persentase-rumah-tangga-menurut-fasilitas-tempat-buang-air-besar-dan-kabupaten-kota-2016-2017.html"
            # "https://papuabarat.bps.go.id/publication/2024/03/14/2bc9211910a8ec2f516cb94d/provinsi-papua-barat-daya-dalam-angka-2024.html "
            # # "https://aceh.bps.go.id/",
            "https://gorontalo.bps.go.id/pressrelease/2023/01/30/916/hasil-long-form-sensus-penduduk-2020-provinsi-gorontalo.html"
            # "https://sumut.bps.go.id/",
            # "https://sumsel.bps.go.id/",
            # "https://sumbar.bps.go.id/"
        ]
        for url in urls:
            # "papuabarat", =  url.split('.')[-4].split('/')[2]
            # if len("papuabarat",) == 3:
            #     "papuabarat", = "papuabarat",.upper()
            # else:
            #     "papuabarat", = "papuabarat",.title()
            
            await page.goto(url, timeout=120000)
            await page.wait_for_timeout(2000)
            
            try:
                popup = await page.query_selector('a[title="Close"]')
                await popup.click()
            except Exception as e:
                logger.debug("no popup")
            
            ranges_item = await page.query_selector_all('ul#yw0 li a')
            if ranges_item:
                try:
                    for idx,ranges in enumerate(ranges_item[0:5], start=1):
                        range_text = await ranges.inner_text()
                        range_text = range_text.replace('\n',' ')
                        link_range = await ranges.get_attribute('href')
                        
                        range_link = f'https://papuabarat.bps.go.id{link_range}'
                        
                        # print(range_link)
                        
                        page4 = await contex.new_page()
                        await page4.goto(f"{range_link}", timeout=120000)
                        unduh = await page.query_selector('//*[@id="PopTriger"]')
                        if unduh:
                            unduh_text = await unduh.inner_text()
                            
                            try:
                                async with page.expect_download() as download_info:
                                    await page.get_by_role("link", name=unduh_text).click()
                                download = await download_info.value

                                title_text = f"Hasil Long Form Sensus Penduduk 2020 Provinsi Gorontalo"
                                
                                nama_file = f"Hasil_Long_Form_Sensus_Penduduk_2020_Provinsi_Gorontalo.pdf"
                                save_path = f"file_excel/{nama_file}"
                                
                                await download.save_as(save_path)
                                
                                file_name_json = f"Hasil_Long_Form_Sensus_Penduduk_2020_Provinsi_Gorontalo.json"
                                json_file_name = f'file_json/{file_name_json}'

                                current_time = int(time.time())
                                data = {
                                    "link": 'https://gorontalo.bps.go.id/pressrelease/2023/01/30/916/hasil-long-form-sensus-penduduk-2020-provinsi-gorontalo.html',
                                    "tag": [
                                        "bps",
                                        "gorontalo",
                                        "tabulardata"
                                    ],
                                    "source": "gorontalo.bps.go.id",
                                    "location_level": "province",
                                    "location": "Gorontalo",
                                    "title": title_text,
                                    "data_name": f"Hasil Long Form Sensus Penduduk 2020 Provinsi Gorontalo",
                                    "range_data": "2020- 2023",
                                    'update': "2023",
                                    "desc": "2023-2020",
                                    "category": "Sosial dan Kependudukan",
                                    "sub_category": "Lingkungan Hidup",
                                    "path_data_raw": [
                                        f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/publikasi/Gorontalo/pdf/{nama_file}',
                                        f's3://ai-pipeline-statistics/data/data_raw/data statistic/BPS/publikasi/Gorontalo/file_json/{file_name_json}'
                                    ],
                                    "file_name": [
                                        f'{nama_file}',
                                        f'{file_name_json}'
                                    ],
                                    "crawling_time_epoch": current_time
                                }
                                
                                if not os.path.exists(f'file_json/'):
                                    os.makedirs(f'file_json/', exist_ok=True)
                                
                                if not os.path.exists(f'file_excel'):
                                    os.makedirs(f'file_excel', exist_ok=True)

                                with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                    json.dump(data, json_file, ensure_ascii=False, indent=4)
                                logger.debug (data)   
                            except Exception as e:
                                logger.debug (e)
                except Exception as e:
                    logger.debug (e)
                
if __name__ == '__main__':
    asyncio.run(main())