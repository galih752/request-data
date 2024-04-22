from datetime import datetime
import json
import time
from playwright.async_api import async_playwright
import asyncio
import s3fs

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
        contex = await browser.new_context()
        page = await contex.new_page()
        await page.goto("https://aksi.bangda.kemendagri.go.id/emonev/DashPrev")
        await asyncio.sleep(2)
        
        for option in range(2, 3):
            opiton = await page.query_selector('//*[@id="_inp_sel_per"]')
            await opiton.select_option(value=f'{option}')
            
            if option == 1:
                tahun = '2019'
            elif option == 2:
                tahun = '2020'
            elif option == 3:
                tahun = '2021'
            elif option == 4:
                tahun = '2022'
            elif option == 5:
                tahun = '2023'
            elif option == 6:
                tahun = '2024'
            print('==================================================')
            print('tahun : ', tahun)
            
            await asyncio.sleep(4)
            
            for i in range(12,31):
                prov = await page.query_selector(f'//*[@id="tProv"]/tbody/tr[{i}]/td[2]')
                nama_prov = await prov.inner_text()
                
                balita = await page.query_selector(f'//*[@id="tProv"]/tbody/tr[{i}]/td[3]')
                jumlah_balita = await balita.inner_text()

                pendek = await page.query_selector(f'//*[@id="tProv"]/tbody/tr[{i}]/td[4]')
                jumlah_pendek = await pendek.inner_text()
                
                sangat_pendek = await page.query_selector(f'//*[@id="tProv"]/tbody/tr[{i}]/td[5]')
                jumlah_sangat_pendek = await sangat_pendek.inner_text()
                
                presentase_stuntin = await page.query_selector(f'//*[@id="tProv"]/tbody/tr[{i}]/td[6]')
                jumlah_presentase_stunting = await presentase_stuntin.inner_text()
                
                print('Provinsi : ', nama_prov)
                
                await prov.click()
                
                # print(nama_prov, ' ', jumlah_balita, ' ', jumlah_pendek, ' ', jumlah_sangat_pendek, ' ', jumlah_presentase_stunting)
                
                await asyncio.sleep(2)
                for k in range(1,101):
                    try:
                        kab = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[2]/div/div/div[2]/ul/div/table/tbody/tr[{k}]/td[2]')
                        nama_kab = await kab.inner_text()
                                
                        balita_kab = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[2]/div/div/div[2]/ul/div/table/tbody/tr[{k}]/td[3]')
                        jumlah_balita_kab = await balita_kab.inner_text()

                        pendek_kab = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[2]/div/div/div[2]/ul/div/table/tbody/tr[{k}]/td[4]')
                        jumlah_pendek_kab = await pendek_kab.inner_text()
                        
                        sangat_pendek_kab = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[2]/div/div/div[2]/ul/div/table/tbody/tr[{k}]/td[5]')
                        jumlah_sangat_pendek_kab = await sangat_pendek_kab.inner_text()
                        
                        relevansi = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[2]/div/div/div[2]/ul/div/table/tbody/tr[{k}]/td[6]')
                        jumlah_relevansi = await relevansi.inner_text()
                        
                        print('Kabupaten : ', nama_kab)
                        
                        await kab.click()
                        # print(nama_kab, ' ', jumlah_balita_kab, ' ', jumlah_pendek_kab, ' ', jumlah_sangat_pendek_kab, ' ', jumlah_relevansi)
                    
                        await asyncio.sleep(2)
                        for d in range(1,800):
                            try:
                                id_dagri = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[3]')
                                id_dagri_text = await id_dagri.inner_text()
                                
                                id_bps = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[2]')
                                id_bps_text = await id_bps.inner_text()
                                
                                des = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[4]')
                                nama_des = await des.inner_text()
                                
                                balita_des = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[5]')
                                jumlah_balita_des = await balita_des.inner_text()
                                
                                pendek_des = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[6]')
                                jumlah_pendek_des = await pendek_des.inner_text()
                                
                                sangat_pendek_des = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[7]')
                                jumlah_sangat_pendek_des = await sangat_pendek_des.inner_text()
                                
                                relevansi_des = await page.query_selector(f'//html/body/div[2]/div[1]/div/div[3]/div/div/div[2]/div/div/table/tbody/tr[{d}]/td[8]')
                                jumlah_relevansi_des = await relevansi_des.inner_text()
                                
                                print('Desa : ', nama_des)
                                print('==================================================')
                                
                                nama_json = f"{nama_prov.replace(' ','_').replace('/','_')}_{nama_kab.replace(' ','_').replace('/','_')}_{nama_des.replace(' ','_').replace('/','_')}_{tahun.replace(' ','_').replace('/','_')}.json"
                                metadata = {
                                    'link':'https://aksi.bangda.kemendagri.go.id/emonev/DashPrev',
                                    'domain':'kemendagri.go.id',
                                    'tag':[
                                        'kemendagri',
                                        'stunting'
                                    ],
                                    'tahun': tahun,
                                    'prov':{
                                        'name':nama_prov,
                                        'jumlah_balita':jumlah_balita,
                                        'stunting_pendek':jumlah_pendek,
                                        'stunting_sangat_pendek':jumlah_sangat_pendek,
                                        'prevalensi_(%)':jumlah_presentase_stunting
                                    },
                                    'kab':{
                                        'name':nama_kab,
                                        'jumlah_balita':jumlah_balita_kab,
                                        'stunting_pendek':jumlah_pendek_kab,
                                        'stunting_sangat_pendek':jumlah_sangat_pendek_kab,
                                        'prevalensi_(%)':jumlah_relevansi
                                    },
                                    'des':{
                                        'name':nama_des,
                                        'id_bps':id_bps_text,
                                        'id_dagri':id_dagri_text,
                                        'jumlah_balita':jumlah_balita_des,
                                        'stunting_pendek':jumlah_pendek_des,
                                        'stunting_sangat_pendek':jumlah_sangat_pendek_des,
                                        'prevalensi_(%)':jumlah_relevansi_des
                                    },
                                    'path_data_raw':f's3://ai-pipeline-statistics/data/data_raw/Kemendagri/angka_prevelensi_stunting/json/{nama_json}',
                                    'crawling_date':datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                                    'crawling_time_epoch':int(time.time())
                                }                                
                                
                                json_s3 = f"s3://ai-pipeline-statistics/data/data_raw/Kemendagri/angka_prevelensi_stunting/json/{nama_json}"
                                json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
                                with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                                    s3_file.write(json_data.encode('utf-8'))
                                print('==================================================')
                                print(f'File {nama_json} berhasil diupload ke S3.')
                                print('==================================================')
                                    
                                print(f'Berhasil menyimpan data {nama_json} ke s3.')
                                print('==================================================')
                                
                                await asyncio.sleep(2)
                            except Exception as e:
                                print(f'{e}  Error bagian ke {d}')
                        kembali = await page.query_selector('//html/body/div[2]/div[1]/div/div[3]/div/div/div[1]/div/a')
                        await kembali.click()

                    except Exception as e:
                            print(f'{e} Error bagian ke {k}')
                kembali = await page.query_selector('//html/body/div[2]/div[1]/div/div[2]/div/div/div[1]/div/a')
                await kembali.click()
        
        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())