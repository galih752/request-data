import s3fs

import json
import os
import time
from datetime import datetime
from requests import Session

from playwright.async_api import async_playwright
import asyncio


huruf_ke = int(input('Masukkan Huruf : '))
urutan_ke = int(input('Masukkan urutan ke : '))

client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

requests: Session = Session()

headers = {
    'authority': 'id.indeed.com',
    'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
    'accept-language': 'en-US,en;q=0.9,id;q=0.8',
    'cookie': 'indeed_rcc=CTK; CTK=1hmdkhhc3j6qk801; CMP_VISITED=1; cmppmeta="eNoBSAC3/zvg+JKypQQOW8XLuTLIZklWiZbVvFdlE6FSnebWuJjjO5xP6OTargRMfGl8ti+lXr5bKBTcIQkpiQKryMeRJN8NBOcFw3k6TFyWI18="; _ga=GA1.2.1664146886.1707707270; _gcl_au=1.1.407010900.1707707270; cp=1; LV="LA=1707707869:CV=1707707869:TS=1707707869"; hpnode=1; indeed_rcc="cmppmeta:CTK"; _gid=GA1.2.2039153868.1708314231; SOCK="EjZ01WQVPl750AxG0HDBzdzAhR8="; SHOE="fLpiKtHzOoCPzeMzj8Cr5wPgCVjCQG_l1KpU3aL9AIDDb7TnGQkwdUOJx7A4x8rwYVl5dJfrubXa6nNPnmlkd3i9WGmfEWZH9rs8zGvFxNxcBihMFPhiKvieBuxF0p-4DYdYjxKC5fwYI98YyWUZCo-iwc-1"; __cf_bm=8V1Qy8XyXI1isxX1pZ.Sv2cqa2sMB07PwIV0YLbfrBA-1708401477-1.0-AQ0DAlxd168oiP4fVPD4DgLzEQlMN0u16GW3aHi2DF82nOKncDguw1BOFmYxbvMDpLcYQfjbg7Nr8bpyj1AUH4Y=; _cfuvid=n2lHakv8jOGVDxNvb0OYmMVT0rohK49qJk3NuCO1N2E-1708401477178-0.0-604800000; INDEED_CSRF_TOKEN=lmjHJOrwexqDQkMPULTfA5cuFBygcrOy; bvcmpgn=id-indeed-com; PPID=eyJraWQiOiJiMGIwZmMxZS1mMmNjLTRlOTQtYTg2ZS0zZDA5MjkyODZlYTEiLCJ0eXAiOiJKV1QiLCJhbGciOiJFUzI1NiJ9.eyJzdWIiOiIzYmZmMzA3ZWU3MzFjZjE5IiwibGFzdF9hdXRoX3RpbWUiOjE3MDgzMTQyNjQ4NjQsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdXRoIjoiZW1haWxPdHAiLCJjcmVhdGVkIjoxNzA3MjA5OTYwMDAwLCJpc3MiOiJodHRwczpcL1wvc2VjdXJlLmluZGVlZC5jb20iLCJsYXN0X2F1dGhfbGV2ZWwiOiJXRUFLIiwibG9nX3RzIjoxNzA4MzE0MjY0ODY0LCJhdWQiOiJjMWFiOGYwNGYiLCJyZW1fbWUiOnRydWUsImV4cCI6MTcwODQwMzI3OCwiaWF0IjoxNzA4NDAxNDc4LCJlbWFpbCI6InJha2FzaXdpZ2FsaWg5NDdAZ21haWwuY29tIn0.OSRwV2U3Xf595s7-IyNUEmVvDQF12lk7lKLbsNSSnQNyoZuzv7WLYqeyJqfdpLkspuGnJBl_KZPXEpKDsInXBA',
    'referer': 'https://id.indeed.com/cmp/A-.l.e/reviews',
    'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'document',
    'sec-fetch-mode': 'navigate',
    'sec-fetch-site': 'same-origin',
    'sec-fetch-user': '?1',
    'upgrade-insecure-requests': '1',
    'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
}

requests.headers.update(headers)

sekarang = datetime.now()

# Format YMD HMS
format_ymd_hms = sekarang.strftime("%Y-%m-%d %H:%M:%S")

async def indeed():
    async with async_playwright() as p:
        browser = await p.firefox.launch(headless=True)
        contex = await browser.new_context()
        page = await contex.new_page()
        await page.set_extra_http_headers(headers)
        
        abjad = huruf_ke
        
        for abjads in abjad:
            print('huruf bagian : ',abjads)
            await page.goto(f'https://id.indeed.com/companies/browse-companies/{abjads}',timeout=60000)
            await asyncio.sleep(15)

            name = await page.query_selector_all('li.css-ckmduv.eu4oa1w0 a.css-1m3zvxq.e19afand0')
            for n,nama in enumerate(name[urutan_ke:]):


                name_company = await nama.get_attribute('href')

                page2 = await contex.new_page()
                await page2.set_extra_http_headers(headers)
                await page2.goto(f'https://id.indeed.com{name_company}',timeout=60000)

                company = await page2.query_selector('//*[@id="cmp-container"]/div/div[1]/header/div[2]/div[3]/div/div/div/div[1]/div[1]/div[2]/div[1]/div/div')
                company_text = await company.inner_text()if company else None

                await asyncio.sleep(1)

                total = await page2.query_selector('div.css-104u4ae.eu4oa1w0')
                total_review = await total.inner_text()if total else None

                page3 = await contex.new_page()

                for pagination in range(0,521,20):
                    try:
                        print('pagination ke :',pagination)
                        await page3.goto(f'https://id.indeed.com{name_company}/reviews?fcountry=ALL&start={pagination}',timeout=60000)
                        await page3.set_extra_http_headers(headers)

                        await asyncio.sleep(5)
                        print(company_text)

                        name_review = await page3.query_selector_all('h2.css-1edqxdo.e1tiznh50 a span span span.css-15r9gu1.eu4oa1w0')
                        location = await page3.query_selector_all('a.css-rt1o3i.e19afand0')
                        categori = await page3.query_selector_all('span.css-xvmbeo.e1wnkr790')
                        content = await page3.query_selector_all('span[itemprop="reviewBody"]')
                        rate = await page3.query_selector_all('button.css-1c33izo.e1wnkr790')
                        for i,(title_review,location_review,categori_review, rate_review,content_review) in enumerate(zip(name_review,location,categori,rate,content)):

                            print('company ke :',n)
                            print('iterasi ke :',i)
                            title_text = await title_review.inner_text()
                            location_text = await location_review.inner_text()
                            categori_text = await categori_review.inner_text()
                            categori_text = categori_text.split('-')
                            categori_text = categori_text[0].strip()
                            review_date = await categori_review.inner_text()
                            review_date = review_date.split('-')
                            review_date = review_date[2].strip()
                            rate_text = await rate_review.inner_text()
                            content_text = await content_review.inner_text()

                            file_name_json = f"reviews_{company_text}"
                            file_name_json = file_name_json.replace(' ', '_') + f'-{pagination}-{i}.json'
                            try:
                                tanggal_objek = datetime.strptime(review_date, "%d %B %Y")

                                # Format tanggal ke format yang diinginkan (yyyy-MM-dd HH:mm:ss)
                                tanggal_hasil = tanggal_objek.strftime("%Y-%m-%d %H:%M:%S")
                                print(tanggal_hasil)
                                epoch_milis = int(tanggal_objek.timestamp() * 1000)
                            except Exception as e:
                                print(e)
                                tanggal_hasil = None
                                epoch_milis = None

                            metadata = {
                                    "link": f'https://id.indeed.com{name_company}',
                                    "domain": "id.indeed.com",
                                    "tag": [
                                    "review",
                                    "company"
                                    ],
                                    "crawling_time": format_ymd_hms,
                                    "crawling_time_epoch": int(time.time()),
                                    "path_data_raw": f"s3://ai-pipeline-statistics/data/data_raw/data_review/indeed/Data review perusahaan/json/{file_name_json}",
                                    "path_data_clean": f"s3://ai-pipeline-statistics/data/data_clean/data_review/indeed/Data review perusahaan/json/{file_name_json}",
                                    "reviews_name": title_text,
                                    "location_reviews": location_text,
                                    "category_reviews": categori_text,
                                    "total_reviews": total_review,
                                    "reviews_rating": {
                                    "total_rating": rate_text,
                                    "detail_total_rating": [{
                                        "score_rating": rate_text,
                                        "category_rating": categori_text
                                    }]
                                    },
                                    "detail_reviews": {
                                    "username_reviews": title_text,
                                    "image_reviews": None,
                                    "created_time": tanggal_hasil,
                                    "created_time_epoch":epoch_milis,
                                    "email_reviews": None,
                                    "company_name": company_text,
                                    "location_reviews": location_text,
                                    "title_detail_reviews": None,
                                    "reviews_rating": rate_text,
                                    "detail_reviews_rating": [{
                                        "score_rating": rate_text,
                                        "category_rating": categori_text
                                    }],
                                    "total_likes_reviews": 0,
                                    "total_dislikes_reviews": 0,
                                    "total_reply_reviews": 0,
                                    "content_reviews": content_text,
                                    "reply_content_reviews": {
                                        "username_reply_reviews": None,
                                        "content_reviews": None,
                                    },
                                    "date_of_experience":None,
                                    "date_of_experience_epoch":None
                                }
                                }
                            # nama_json = os.path.join('indeed/json', file_name_json)

                            # with open(nama_json, 'w', encoding='utf-8') as file:
                            #     json.dump(metadata, file, indent=4)

                            print(metadata)
                            with open(f'log.txt', 'a') as log_file:
                                log_file.write(f'huruf bagian : {abjads}\ncompany ke : {n}\niterasi ke: {i}\npagination ke : {pagination} Success saved!')


                            json_s3 = f'ai-pipeline-statistics/data/data_raw/data_review/indeed/Data review perusahaan/json/{file_name_json}'
                            json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
                            with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                                s3_file.write(json_data.encode('utf-8'))
                            print('==================================================')
                            print(f'File {file_name_json} berhasil diupload ke S3.')
                            print('==================================================')
                            print('Urutan ke :',urutan_ke)
                            print('Huruf :',huruf_ke)
                            print('==================================================')
                    except Exception as e:
                        print(e)

                await page2.close()
        await page.close()



if __name__=='__main__':
    asyncio.run(indeed())
