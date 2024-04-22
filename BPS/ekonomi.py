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
            "https://aceh.bps.go.id/",
            "https://sumut.bps.go.id/",
            "https://sumsel.bps.go.id/",
            "https://sumbar.bps.go.id/"
        ]
        for url in urls:
            province_name =  url.split('.')[-4].split('/')[2]
            if len(province_name) == 3:
                province_name = province_name.upper()
            else:
                province_name = province_name.title()
            
            await page.goto(url, timeout=120000)
            await page.wait_for_timeout(2000)
            
            try:
                popup = await page.query_selector('a[title="Close"]')
                await popup.click()
            except Exception as e:
                logger.debug("no popup")
            
            kategori = await page.query_selector('//*[@id="tab_item-1"]')
            kategori_text = await kategori.inner_text()
            kategori_text = kategori_text.replace('\n',' ')
            await kategori.click()
            
            #EKONOMI (KEDUA)
            for i in range(1,30):
                await kategori.click()
                sub = await page.query_selector(f'//*[@id="ekonomi"]/p[{i}]/small/a')
                sub_category = await sub.inner_text()
                sub_category = sub_category.replace('\n',' ')
                sub_category_link = await sub.get_attribute('href')
                
                sub_link = ''
                if url in sub_category_link:
                    sub_link += sub_category_link
                else:
                    sub_link = f"{url}{sub_category_link}"
                
                page2 = await contex.new_page()
                await page2.goto(f"{sub_link}", timeout=120000)
                
                select = await page2.query_selector('//*[@id="listTabel1_length"]/label/select')
                await select.select_option(value="-1")
                
                titles = await page2.query_selector_all('tr[role="row"] td a')
                updated = await page2.query_selector_all('tr[role="row"] td.text-center.sorting_1')
                desc = await page2.query_selector_all('tr[role="row"] td span')
                for title,update,description in zip(titles,updated,desc):
                    title_text = await title.inner_text()
                    title_text = title_text.replace('\n',' ')
                    link_item = await title.get_attribute('href')
                    update_text = await update.inner_text()
                    update_text = update_text.replace('\n',' ')
                    description_text = await description.inner_text()
                    description_text = description_text.replace('\n',' ')
                    
                    title_link = ''
                    if f"{url}" in link_item:
                        title_link += link_item
                    else:
                        title_link += f"{url}{link_item}"
                        
                    page3 = await contex.new_page()
                    await page3.goto(f"{title_link}", timeout=120000)

                    data = await page3.query_selector('//*[@id="column2"]/h4')
                    data_name = await data.inner_text()if data else f"{re.sub(r'[^a-zA-Z0-9]+', '_', title_text)}"
                    data_name = data_name.replace('\n',' ')
                    
                    ranges_item = await page3.query_selector_all('ul#yw0 li a')
                    if ranges_item:
                        try:
                            for ranges in ranges_item:
                                range_text = await ranges.inner_text()
                                range_text = range_text.replace('\n',' ')
                                link_range = await ranges.get_attribute('href')
                                
                                range_link = ''
                                if url in link_range:
                                    range_link += link_range
                                else:
                                    range_link += f"{url}{link_range}"
                                
                                page4 = await contex.new_page()
                                await page4.goto(f"{range_link}", timeout=120000)

                                name_locator = await page4.query_selector('//*[@id="column2"]/div[1]/a')
                                name_text = await name_locator.inner_text()
                                
                                pdf_satu = await page4.query_selector('//*[@id="column2"]/div[1]/button')
                                try:
                                    if pdf_satu:
                                        async with page4.expect_download() as download_info:
                                                await page4.get_by_role("button", name=" xlsx").click()
                                        download = await download_info.value
                                    
                                        nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', data_name)}({range_text}).xlsx"
                                        nama_file = truncate_filename(nama_file)
                                        save_path = f"province/{province_name}/{kategori_text}/{sub_category}/file_excel/{nama_file}"
                                        
                                        await download.save_as(save_path)
                                        
                                        file_name_json = f'{re.sub(r"[^a-zA-Z0-9]+", "_", data_name)}({range_text}).json'
                                        file_name_json = truncate_filename(file_name_json)
                                        json_file_name = f'province/{province_name}/{kategori_text}/{sub_category}/file_json/{file_name_json}'

                                        current_time = int(time.time())
                                        data = {
                                            "link": f"{title_link}",
                                            "tag": [
                                                "bps",
                                                province_name,
                                                "tabulardata"
                                            ],
                                            "source": f"{province_name.lower()}.bps.go.id",
                                            "location_level": "province",
                                            "location": province_name,
                                            "title": title_text,
                                            "data_name": f'{data_name}',
                                            "range_data": range_text,
                                            "update": update_text,
                                            "desc": description_text,
                                            "category": kategori_text,
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
                                        
                                        if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_json'):
                                            os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_json', exist_ok=True)
                                        
                                        if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel'):
                                            os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel', exist_ok=True)

                                        with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                            json.dump(data, json_file, ensure_ascii=False, indent=4)
                                        logger.debug (data)
                                        await page4.close()
                                    else:
                                        await asyncio.sleep(1)
                                        pdf_dua = await page4.query_selector('//*[@id="column2"]/div[1]/a')
                                        if pdf_dua:
                                            async with page4.expect_download() as download_info:
                                                    await pdf_dua.click()
                                            download = await download_info.value
                                        
                                            nama_file_dua = f"{re.sub(r'[^a-zA-Z0-9]+', '_', data_name)}({range_text}).xls"
                                            nama_file_dua = truncate_filename(nama_file_dua)
                                            save_path_dua = f"province/{province_name}/{kategori_text}/{sub_category}/file_excel/{nama_file_dua}"

                                            await download.save_as(save_path_dua)
                                            
                                            file_name_json = f'{re.sub(r"[^a-zA-Z0-9]+", "_", data_name)}({range_text}).json'
                                            file_name_json = truncate_filename(file_name_json)
                                            json_file_name = f'province/{province_name}/{kategori_text}/{sub_category}/file_json/{file_name_json}'

                                            current_time = int(time.time())
                                            data = {
                                                "link": f"{title_link}",
                                                "tag": [
                                                    "bps",
                                                    province_name,
                                                    "tabulardata"
                                                ],
                                                "source": f"{province_name.lower()}.bps.go.id",
                                                "location_level": "province",
                                                "location": province_name,
                                                "title": title_text,
                                                "data_name": f'{data_name}',
                                                "range_data": range_text,
                                                "update": update_text,
                                                "desc": description_text,
                                                "category": kategori_text,
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
                                            
                                            if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_json'):
                                                os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_json', exist_ok=True)
                                            
                                            if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel'):
                                                os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel', exist_ok=True)

                                            with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                                json.dump(data, json_file, ensure_ascii=False, indent=4)
                                            logger.debug (data)
                                            await page4.close()
                                            
                                except Exception as e:
                                    logger.debug(e)
                        except Exception as e:
                            logger.debug(e)
                    else:
                        selects = await page3.query_selector_all('//*[@id="column2"]/div[3]/form/select/option')
                        try:
                            for select in selects[1:]:
                                range_text = await select.inner_text()
                                select_link = await select.get_attribute('value')

                                select_option = ''
                                if url in select_link:
                                    select_option += select_link
                                else:
                                    select_option += f"{url}{select_link}"
                                    
                                logger.debug(select_option)
                                page4 = await contex.new_page()
                                await page4.goto(f"{select_option}", timeout=120000)
                                    
                                pdf_satu = await page4.query_selector('//*[@id="column2"]/div[1]/button')
                                try:
                                    if pdf_satu:
                                        async with page4.expect_download() as download_info:
                                                await page4.get_by_role("button", name=" xlsx").click()
                                        download = await download_info.value
                                    
                                        nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', data_name)}({range_text}).xlsx"
                                        nama_file = truncate_filename(nama_file)
                                        save_path = f"province/{province_name}/{kategori_text}/{sub_category}/file_excel/{nama_file}"
                                        
                                        await download.save_as(save_path)
                                        
                                        file_name_json = f'{re.sub(r"[^a-zA-Z0-9]+", "_", data_name)}({range_text}).json'
                                        file_name_json = truncate_filename(file_name_json)
                                        json_file_name = f'province/{province_name}/{kategori_text}/{sub_category}/file_json/{file_name_json}'

                                        current_time = int(time.time())
                                        data = {
                                            "link": f"{title_link}",
                                            "tag": [
                                                "bps",
                                                province_name,
                                                "tabulardata"
                                            ],
                                            "source": f"{province_name.lower()}.bps.go.id",
                                            "location_level": "province",
                                            "location": province_name,
                                            "title": title_text,
                                            "data_name": f'{data_name}',
                                            "range_data": range_text,
                                            "update": update_text,
                                            "desc": description_text,
                                            "category": kategori_text,
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
                                        
                                        if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_json'):
                                            os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_json', exist_ok=True)
                                        
                                        if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel'):
                                            os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel', exist_ok=True)

                                        with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                            json.dump(data, json_file, ensure_ascii=False, indent=4)
                                        logger.debug (data)
                                        await page4.close()
                                    else:
                                        await asyncio.sleep(1)
                                        pdf_dua = await page4.query_selector('//*[@id="column2"]/div[1]/a')
                                        if pdf_dua:
                                            async with page4.expect_download() as download_info:
                                                    await pdf_dua.click()
                                            download = await download_info.value
                                        
                                            nama_file_dua = f"{re.sub(r'[^a-zA-Z0-9]+', '_', data_name)}({range_text}).xls"
                                            nama_file_dua = truncate_filename(nama_file_dua)
                                            save_path_dua = f"province/{province_name}/{kategori_text}/{sub_category}/file_excel/{nama_file_dua}"

                                            await download.save_as(save_path_dua)
                                            
                                            file_name_json = f'{re.sub(r"[^a-zA-Z0-9]+", "_", data_name)}({range_text}).json'
                                            file_name_json = truncate_filename(file_name_json)
                                            json_file_name = f'province/{province_name}/{kategori_text}/{sub_category}/file_json/{file_name_json}'

                                            current_time = int(time.time())
                                            data = {
                                                "link": f"{title_link}",
                                                "tag": [
                                                    "bps",
                                                    province_name,
                                                    "tabulardata"
                                                ],
                                                "source": f"{province_name.lower()}.bps.go.id",
                                                "location_level": "province",
                                                "location": province_name,
                                                "title": title_text,
                                                "data_name": f'{data_name}',
                                                "range_data": range_text,
                                                "update": update_text,
                                                "desc": description_text,
                                                "category": kategori_text,
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
                                            
                                            if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_json'):
                                                os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_json', exist_ok=True)
                                            
                                            if not os.path.exists(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel'):
                                                os.makedirs(f'province/{province_name}/{kategori_text}/{sub_category}/file_excel', exist_ok=True)

                                            with open(json_file_name, 'w', encoding='utf-8') as json_file:
                                                json.dump(data, json_file, ensure_ascii=False, indent=4)
                                            logger.debug (data)
                                            await page4.close()
                                except Exception as e:
                                    logger.debug(e)
                            await page4.close()
                        except Exception as e:
                            logger.debug(e)
                    await page3.close()
                await page2.close()
            await browser.close()
if __name__ == "__main__":
    asyncio.run(main())