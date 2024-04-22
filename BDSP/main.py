from datetime import datetime
import json
import os
import re
import time
from playwright.async_api import async_playwright
import s3fs
import asyncio

client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto("https://bdsp2.pertanian.go.id/bdsp/id/home.html")

        await page.wait_for_selector('#subsektor')
        await asyncio.sleep(5)
        
        for option in range(1, 5):
            select = await page.query_selector('#subsektor')
            await select.select_option(f'0{option}')
            if option == 1:
                subsector = 'Tanaman Pangan'
            elif option == 2:
                subsector = 'Perkebunan'
            elif option == 3:
                subsector = 'Peternakan'
            elif option == 4:
                subsector = 'Hortikultura'

            await page.wait_for_selector('#komoditas')
            await page.click('#komoditas')
            await asyncio.sleep(1)
            
            option_komo = await page.query_selector_all('select#komoditas option')
            for option in option_komo[1:]:
                option_komo_text = await option.get_attribute('value')
                option_satu = await option.inner_text()
                await page.select_option('select#komoditas', value=option_komo_text)
                print(f'komoditas = {option_komo_text}')

                await page.wait_for_selector('#indikator')
                await page.click('#indikator')
                await asyncio.sleep(1)

                option_indi = await page.query_selector_all('select#indikator option')
                for option in option_indi[1:]:
                    option_indi_text = await option.get_attribute('value')
                    option_dua = await option.inner_text()
                    await page.select_option('select#indikator', value=option_indi_text)
                    print(f'indikator = {option_indi_text}')

                    await page.wait_for_selector('#tahunAwal')
                    await page.click('#tahunAwal')
                    await asyncio.sleep(1)

                    option_tahun_awal = await page.query_selector_all('select#tahunAwal option')
                    option_tahun_akhir = await page.query_selector_all('select#tahunAkhir option')
                    
                    for option1, option2 in zip(option_tahun_awal[1:], option_tahun_akhir[1:]):
                        option_tahun_awal_text = await option1.get_attribute('value')
                        option_tiga = await option1.inner_text()
                        await page.select_option('select#tahunAwal', value=option_tahun_awal_text)
                        print(f'tahun awal = {option_tahun_awal_text}')

                        await page.wait_for_selector('#tahunAkhir')
                        await page.click('#tahunAkhir')
                        await asyncio.sleep(1)
                        option_tahun_akhir_text = await option2.get_attribute('value')
                        option_empat = await option2.inner_text()
                        await page.select_option('select#tahunAkhir', value=option_tahun_akhir_text)
                        print(f'tahun akhir = {option_tahun_akhir_text}')

                        await asyncio.sleep(1)
                        
                        cari = await page.query_selector('//*[@id="searchDashboard"]')
                        await cari.click()
                        
                        await asyncio.sleep(3)
                        
                        
                        #Chart 1
                        await page.get_by_label("View chart menu, Peta Sebaran").click()
                        
                        await asyncio.sleep(2)
                        
                        async with page.expect_download() as download_info:
                            await page.get_by_text("Download XLS").click(modifiers=["Alt"])
                        download = await download_info.value
                        
                        await asyncio.sleep(25)

                        # Save the file locally
                        file_path = os.path.join(os.getcwd(), 'temp_download.xls')
                        await download.save_as(file_path)

                        # Upload the file to S3
                        nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(1).xls"
                        nama_file = nama_file.replace(' ', '_')
                        save_path = f"ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}"
                        with s3.open(save_path, 'wb') as f:
                            f.write(open(file_path, 'rb').read())

                        os.remove(file_path)
                        print(" File Berhasil Disimpan ke S3!")
                        
                        file_json = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(1).json"
                        
                        metadata = {
                            'link': 'https://bdsp2.pertanian.go.id/bdsp/id/home.html',
                            'domain': 'bdsp2.pertanian.go.id',
                            'tag':[
                                'Luas Panen dan Produksi',
                                'bdsp2pertaniangoid'
                            ],
                            'file_name': nama_file,
                            'path_data_raw': [
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}',
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                                ],
                            'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'crawling_time_epoch': int(time.time()),
                        }
                        
                        json_s3 = f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
                        with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                            s3_file.write(json_data.encode('utf-8'))
                        print('==================================================')
                        print(f'File {file_json} berhasil diupload ke S3.')
                        print('==================================================')
                            
                        print(f'Berhasil menyimpan data {file_json}.json')
                        
                        
                        cari = await page.query_selector('//*[@id="searchDashboard"]')
                        await cari.click()
                        
                        await asyncio.sleep(3)
                        
                        #CHART 2
                        await page.get_by_label("View chart menu, Produksi dan").click()
                        
                        await asyncio.sleep(2)
                        
                        async with page.expect_download() as download_info:
                            await page.get_by_text("Download XLS").click(modifiers=["Alt"])
                        download = await download_info.value
                        
                        await asyncio.sleep(25)

                        # Save the file locally
                        file_path = os.path.join(os.getcwd(), 'temp_download.xls')
                        await download.save_as(file_path)

                        # Upload the file to S3
                        nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(2).xls"
                        nama_file = nama_file.replace(' ', '_')
                        save_path = f"ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}"
                        with s3.open(save_path, 'wb') as f:
                            f.write(open(file_path, 'rb').read())

                        os.remove(file_path)
                        print(" File Berhasil Disimpan ke S3!")
                        
                        file_json = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(2).json"
                        
                        metadata = {
                            'link': 'https://bdsp2.pertanian.go.id/bdsp/id/home.html',
                            'domain': 'bdsp2.pertanian.go.id',
                            'tag':[
                                'Luas Panen dan Produksi',
                                'bdsp2pertaniangoid'
                            ],
                            'file_name': nama_file,
                            'path_data_raw': [
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}',
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                                ],
                            'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'crawling_time_epoch': int(time.time()),
                        }
                        
                        json_s3 = f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
                        with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                            s3_file.write(json_data.encode('utf-8'))
                        print('==================================================')
                        print(f'File {file_json} berhasil diupload ke S3.')
                        print('==================================================')
                            
                        print(f'Berhasil menyimpan data {file_json}.json')
                        
                        cari = await page.query_selector('//*[@id="searchDashboard"]')
                        await cari.click()
                        
                        await asyncio.sleep(3)
                        
                        #CHART 3
                        await page.get_by_label("View chart menu, Kontribusi").click()
                        
                        await asyncio.sleep(2)
                        
                        async with page.expect_download() as download_info:
                            await page.get_by_text("Download XLS").click(modifiers=["Alt"])
                        download = await download_info.value
                        
                        await asyncio.sleep(25)

                        # Save the file locally
                        file_path = os.path.join(os.getcwd(), 'temp_download.xls')
                        await download.save_as(file_path)

                        # Upload the file to S3
                        nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(3).xls"
                        nama_file = nama_file.replace(' ', '_')
                        save_path = f"ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}"
                        with s3.open(save_path, 'wb') as f:
                            f.write(open(file_path, 'rb').read())

                        os.remove(file_path)
                        print(" File Berhasil Disimpan ke S3!")
                        
                        file_json = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(3).json"
                        
                        metadata = {
                            'link': 'https://bdsp2.pertanian.go.id/bdsp/id/home.html',
                            'domain': 'bdsp2.pertanian.go.id',
                            'tag':[
                                'Luas Panen dan Produksi',
                                'bdsp2pertaniangoid'
                            ],
                            'file_name': nama_file,
                            'path_data_raw': [
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/excel/{nama_file}',
                                f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                                ],
                            'crawling_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'crawling_time_epoch': int(time.time()),
                        }
                        
                        json_s3 = f's3://ai-pipeline-statistics/data/data_raw/bdsp pertanian/data luas panen dan produksi komoditas/json/{file_json}'
                        json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
                        with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                            s3_file.write(json_data.encode('utf-8'))
                        print('==================================================')
                        print(f'File {file_json} berhasil diupload ke S3.')
                        print('==================================================')
                            
                        print(f'Berhasil menyimpan data {file_json}.json')
                        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
