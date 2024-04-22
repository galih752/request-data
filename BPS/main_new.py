import json
import os
import s3fs
import time
from playwright.sync_api import Playwright, sync_playwright, expect
import requests

client_kwargs = {
                'key': 'GLZG2JTWDFFSCQVE7TSQ',
                'secret': 'VjTXOpbhGvYjDJDAt2PNgbxPKjYA4p4B7Btmm4Tw',
                'endpoint_url': 'http://192.168.180.9:8000',
                'anon': False
            }

s3 = s3fs.core.S3FileSystem(**client_kwargs)

mapping_name = {
        "ntt": "Nusa Tenggara Timur",
        "malut": "Maluku Utara",
        "banten": "Banten",
        "sultra": "Sulawesi Tenggara",
        "babel": "Kepulauan Bangka Belitung",
        "papua tengah": "Papua Tengah",
        "papua selatan": "Papua Selatan",
        "papuabarat": "Papua Barat",
        "kepri": "Kepulauan Riau",
        "sumsel": "Sumatera Selatan",
        "maluku": "Maluku",
        "jateng": "Jawa Tengah",
        "gorontalo": "Gorontalo",
        "sulbar": "Sulawesi Barat",
        "ntb": "Nusa Tenggara Barat",
        "kalsel": "Kalimantan Selatan",
        "kalbar": "Kalimantan Barat",
        "kalteng": "Kalimantan Tengah",
        "papua": "Papua",
        "sulut": "Sulawesi Utara",
        "jakarta": "DKI Jakarta",
        "sulteng": "Sulawesi Tengah",
        "sumut": "Sumatera Utara",
        "kaltim": "Kalimantan Timur",
        "yogyakarta": "Daerah Istimewa Yogyakarta",
        "aceh": "Aceh",
        "sulsel": "Sulawesi Selatan",
        "jatim": "Jawa Timur",
        "kaltara": "Kalimantan Utara",
        "sumbar": "Sumatera Barat",
        "jambi": "Jambi"
    }


urls = [
    # 'https://papuabarat.bps.go.id/publication/2024/03/14/2bc9211910a8ec2f516cb94d/provinsi-papua-barat-daya-dalam-angka-2024.html',
    # 'https://papua.bps.go.id/publication/2024/02/28/cb8c29a69d5c6f9b146e6375/provinsi-papua-pegunungan-dalam-angka-2024.html',
    'https://papuabarat.bps.go.id/publication/2024/02/28/1abcd220e6df6bf3af905766/provinsi-papua-barat-dalam-angka-2024.html',
    # 'https://sulteng.bps.go.id/publication/2023/02/28/6eadd334118ff89b9957b958/provinsi-sulawesi-tengah-dalam-angka-2023.html',
    # 'https://sulteng.bps.go.id/publication/2022/02/25/d8cccf7c0b42c3d9ff80b8c6/provinsi-sulawesi-tengah-dalam-angka-2022.html',
    # 'https://sulteng.bps.go.id/publication/2024/02/28/a7d8b5a36c8bb23fbde3cde1/provinsi-sulawesi-tengah-dalam-angka-2024.html'
    # 'https://sulteng.bps.go.id/publication/2024/02/28/a7d8b5a36c8bb23fbde3cde1/provinsi-sulawesi-tengah-dalam-angka-2024.html',
    # 'https://sulsel.bps.go.id/publication/2023/02/28/3ea69ff21d346fa74bb816b9/provinsi-sulawesi-selatan-dalam-angka-2023.html'
    # 'https://kalteng.bps.go.id/publication/2023/07/14/86b433069f2a64d9e83292e7/statistik-migrasi-provinsi-kalimantan-tengah-hasil-long-form-sensus-penduduk-2020.html',
    # 'https://jambi.bps.go.id/publication/2023/07/14/a09ddfdc0519c138d79d7a60/statistik-migrasi-jambi-hasil-long-form-sensus-penduduk-2020.html',
    # 'https://ntt.bps.go.id/publication/2023/07/14/3f73d9c9fbcadb33dadd201c/statistik-migrasi-provinsi-nusa-tenggara-timur-hasil-long-form-sensus-penduduk-2020.html',
    # 'https://malut.bps.go.id/publication/2022/02/21/f14cf77f9475fbe12c7a5663/keadaan-angkatan-kerja-di-provinsi-maluku-utara-agustus-2021.html',
    # 'https://malut.bps.go.id/publication/2023/07/14/7728746bd3f0106d09557110/statistik-migrasi-provinsi-maluku-utara-hasil-long-form-sensus-penduduk-2020.html',
    # 'https://banten.bps.go.id/publication/2023/07/14/6b4b81c576efc43360ef3d29/statistik-migrasi-provinsi-banten-hasil-long-form-sensus-penduduk-2020.html',
    # 'https://sultra.bps.go.id/publication/2023/07/14/d6c389cfa01255a559c9fa2e/statistik-migrasi-provinsi-sulawesi-tenggara-hasil-long-form-sensus-penduduk-2020.html'
    # "https://babel.bps.go.id/publication/2023/05/05/1638685bfa79e9bb2e69fae7/keadaan-angkatan-kerja-provinsi-kepulauan-bangka-belitung-agustus-2022.html",
    # "https://babel.bps.go.id/publication/2023/07/14/06e1fdbe4bcb304c43c059c1/statistik-migrasi-provinsi-kepulauan-bangka-belitung-hasil-long-form-sensus-penduduk-2020.html",
    # "https://papua.bps.go.id/publication/2024/02/28/d3863914517dec264cf62df2/provinsi-papua-tengah-dalam-angka-2024.html",
    # "https://papua.bps.go.id/publication.html?Publikasi%5BtahunJudul%5D=&Publikasi%5BkataKunci%5D=papua+tengah&Publikasi%5BcekJudul%5D=0&yt0=Tampilkan",
    # "https://papua.bps.go.id/publication/2023/07/14/04d030076fcdbd463369d3b7/penduduk-provinsi-papua-hasil-long-form-sensus-penduduk-2020.html",
    # "https://papua.bps.go.id/publication/2023/07/14/fb4c0814a6a0b5ec5bea4ae8/statistik-migrasi-papua-hasil-long-form-sensus-penduduk-2020.html",
    # "https://kepri.bps.go.id/publication/2023/07/14/7d480dbc0af0fe4354d105d3/statistik-migrasi-provinsi-kepulauan-riau-hasil-long-form-sensus-penduduk-2020.html",
    # "https://sumsel.bps.go.id/publication/2024/02/28/24b0b0a6676d1d095ab88ce2/provinsi-sumatera-selatan-dalam-angka-2024.html",
    # "https://sumsel.bps.go.id/publication/2024/02/28/24b0b0a6676d1d095ab88ce2/provinsi-sumatera-selatan-dalam-angka-2024.html",
    # "https://sumsel.bps.go.id/publication/2023/07/14/234aa7d95561c80b6badd02b/statistik-migrasi-provinsi-sumatera-selatan-hasil-long-form-sensus-penduduk-2020.html",
    # "https://papua.bps.go.id/publication/2024/02/28/de1c58318d7d8ba84c8b3f56/provinsi-papua-dalam-angka-2024.html",
    # "https://papua.bps.go.id/publication/2023/07/14/fb4c0814a6a0b5ec5bea4ae8/statistik-migrasi-papua-hasil-long-form-sensus-penduduk-2020.html",
    # "https://maluku.bps.go.id/publication/2023/02/28/5e8944e1ca42a5199c4c577e/provinsi-maluku-dalam-angka-2023.html",
    # "https://maluku.bps.go.id/publication/2023/07/14/e078ca131935233ee2edad8d/statistik-migrasi-provinsi-maluku-hasil-long-form-sensus-penduduk-2020.html",
    # "https://jateng.bps.go.id/publication/2023/02/28/754e4785496c09ab1f787570/provinsi-jawa-tengah-dalam-angka-2023.html",
    # "https://jateng.bps.go.id/publication/2023/07/14/2da5f15b493c591545551ebf/statistik-migrasi-provinsi-jawa-tengah--hasil-long-form-sensus-penduduk-2020.html",
    # "https://gorontalo.bps.go.id/publication/2023/07/14/487185d2342dbad3525491c1/statistik-migrasi-provinsi-gorontalo-hasil-long-form-sensus-penduduk-2020.html",
    # "https://sulbar.bps.go.id/publication/2023/02/28/d55348777d746c1f43302d83/provinsi-sulawesi-barat-dalam-angka-2023.html",
    # "https://sulbar.bps.go.id/publication/2023/07/14/28ad234428cf2b18e4aef33e/statistik-migrasi-provinsi-sulawesi-barat-hasil-long-form-sensus-penduduk-2020.html",
    # "https://ntb.bps.go.id/publication/2023/07/14/4df4e66b7128bc849fb89ede/statistik-migrasi-provinsi-nusa-tenggara-barat-hasil-long-form-sensus-penduduk-2020.html",
    # "https://kalsel.bps.go.id/publication/2022/02/25/2d212455a03806fc8befa9f5/provinsi-kalimantan-selatan-dalam-angka-2022.html",
    # "https://kalsel.bps.go.id/publication/2024/02/28/a385e874f0cddcf61d8adea0/provinsi-kalimantan-selatan-dalam-angka-2024.html",
    # "https://kalsel.bps.go.id/publication/2023/07/14/1b87faf44f46ab63da9e262f/statistik-migrasi-provinsi-kalimantan-selatan-hasil-long-form-sensus-penduduk-2020.html",
    # "https://kalbar.bps.go.id/publication/2023/07/14/639c0857e7fb829294e567b4/statistik-migrasi-kalimantan-barat-hasil-long-form-sensus-penduduk-2020.html",
    # "https://papuabarat.bps.go.id/publication/2023/07/14/feab456b790b1ab16ae43c2a/statistik-migrasi-provinsi-papua-barat--hasil-long-form-sensus-penduduk-2020-.html",
    # "https://sulut.bps.go.id/publication/2023/07/14/01e3df5d59f54d4683fefee2/statistik-migrasi-provinsi-sulawesi-utara-hasil-long-form-sensus--penduduk-2020.html",
    # "https://jakarta.bps.go.id/publication/2023/07/14/3c2cab79e5e41e2db26f96f2/statistik-migrasi-provinsi-dki-jakarta-hasil-long-form-sensus-penduduk-2020.html",
    # # "https://sulteng.bps.go.id/publication/download.html?nrbvfeve=YTdkOGI1YTM2YzhiYjIzZmJkZTNjZGUx&xzmn=aHR0cHM6Ly9zdWx0ZW5nLmJwcy5nby5pZC9wdWJsaWNhdGlvbi8yMDI0LzAyLzI4L2E3ZDhiNWEzNmM4YmIyM2ZiZGUzY2RlMS9wcm92aW5zaS1zdWxhd2VzaS10ZW5nYWgtZGFsYW0tYW5na2EtMjAyNC5odG1s&twoadfnoarfeauf=MjAyNC0wMy0xNiAyMToxODo0NA%3D%3D",
    # # "https://sulteng.bps.go.id/publication/download.html?nrbvfeve=NmVhZGQzMzQxMThmZjg5Yjk5NTdiOTU4&xzmn=aHR0cHM6Ly9zdWx0ZW5nLmJwcy5nby5pZC9wdWJsaWNhdGlvbi8yMDIzLzAyLzI4LzZlYWRkMzM0MTE4ZmY4OWI5OTU3Yjk1OC9wcm92aW5zaS1zdWxhd2VzaS10ZW5nYWgtZGFsYW0tYW5na2EtMjAyMy5odG1s&twoadfnoarfeauf=MjAyNC0wMy0xNiAyMjoxMjo0NA%3D%3D",
    # # "https://sulteng.bps.go.id/publication/download.html?nrbvfeve=ZDhjY2NmN2MwYjQyYzNkOWZmODBiOGM2&xzmn=aHR0cHM6Ly9zdWx0ZW5nLmJwcy5nby5pZC9wdWJsaWNhdGlvbi8yMDIyLzAyLzI1L2Q4Y2NjZjdjMGI0MmMzZDlmZjgwYjhjNi9wcm92aW5zaS1zdWxhd2VzaS10ZW5nYWgtZGFsYW0tYW5na2EtMjAyMi5odG1s&twoadfnoarfeauf=MjAyNC0wMy0xNiAyMjoxNDo1MA%3D%3D",
    # # "https://sulteng.bps.go.id/publication/download.html?nrbvfeve=YTdkOGI1YTM2YzhiYjIzZmJkZTNjZGUx&xzmn=aHR0cHM6Ly9zdWx0ZW5nLmJwcy5nby5pZC9wdWJsaWNhdGlvbi8yMDI0LzAyLzI4L2E3ZDhiNWEzNmM4YmIyM2ZiZGUzY2RlMS9wcm92aW5zaS1zdWxhd2VzaS10ZW5nYWgtZGFsYW0tYW5na2EtMjAyNC5odG1s&twoadfnoarfeauf=MjAyNC0wMy0xNyAwMToxNTo0MA%3D%3D",
    # "https://sulteng.bps.go.id/publication/2024/02/28/a7d8b5a36c8bb23fbde3cde1/provinsi-sulawesi-tengah-dalam-angka-2024.html",
    # "https://sulteng.bps.go.id/publication/2023/02/28/6eadd334118ff89b9957b958/provinsi-sulawesi-tengah-dalam-angka-2023.html",
    # "https://sulteng.bps.go.id/publication/2022/02/25/d8cccf7c0b42c3d9ff80b8c6/provinsi-sulawesi-tengah-dalam-angka-2022.html",
    #  "https://sumut.bps.go.id/publication/2024/02/28/a2b9ed5089227612befc7827/provinsi-sumatera-utara-dalam-angka-2024.html",
    # "https://sumut.bps.go.id/publication/2023/02/28/ee319bd16e8eaee7599bfaa7/provinsi-sumatera-utara-dalam-angka-2023.html",
    # "https://sumut.bps.go.id/publication/2023/07/14/fe91a50c6552c8ff67de48ba/statistik-migrasi-provinsi-sumatera-utara-hasil-long-form-sensus-penduduk-2020.html",
    # "https://kaltim.bps.go.id/publication/2024/02/28/09130670899667e6766c711c/provinsi-kalimantan-timur-dalam-angka-2024.html",
    # "https://kaltim.bps.go.id/publication/2023/02/28/7a58231d5aa2f5a7b4d5c36a/provinsi-kalimantan-timur-dalam-angka-2023.html",
    # "https://kaltim.bps.go.id/publication/2022/02/25/30d7f530df6226df3d80356e/provinsi-kalimantan-timur-dalam-angka-2022.html",
    # "https://kaltim.bps.go.id/publication/2023/07/14/0cc3d5368c2eeb3d7859f182/statistik-migrasi-provinsi-kalimantan-timur-hasil-long-form-sensus-penduduk-2020.html",
    # "https://yogyakarta.bps.go.id/publication/2023/07/14/faaf2e40809c429f3b8935e4/statistik-migrasi-provinsi-daerah-istimewa-yogyakarta-hasil-long-form-sensus-penduduk-2020.html",
    # "https://aceh.bps.go.id/publication/2023/07/18/15f436a4c38dff22674f4ba7/statistik-migrasi-provinsi-aceh-hasil-long-form-sensus-penduduk-2020.html",
    # "https://aceh.bps.go.id/publication/2023/07/18/15f436a4c38dff22674f4ba7/statistik-migrasi-provinsi-aceh-hasil-long-form-sensus-penduduk-2021.html",
    # "https://aceh.bps.go.id/publication/2023/07/18/15f436a4c38dff22674f4ba7/statistik-migrasi-provinsi-aceh-hasil-long-form-sensus-penduduk-2022.html",
    # "https://sulsel.bps.go.id/publication/2023/07/14/097ea3241b6af1015239a4a8/statistik-migrasi-penduduk-sulawesi-selatan-hasil-long-form-sensus-penduduk-2020.html",
    # "https://sulsel.bps.go.id/publication/2023/07/14/097ea3241b6af1015239a4a8/statistik-migrasi-penduduk-sulawesi-selatan-hasil-long-form-sensus-penduduk-2021.html",
    # "https://sulsel.bps.go.id/publication/2023/07/14/097ea3241b6af1015239a4a8/statistik-migrasi-penduduk-sulawesi-selatan-hasil-long-form-sensus-penduduk-2022.html",
    # "https://sulsel.bps.go.id/publication/2023/07/14/097ea3241b6af1015239a4a8/statistik-migrasi-penduduk-sulawesi-selatan-hasil-long-form-sensus-penduduk-2023.html",
    # "https://sulsel.bps.go.id/publication/2023/07/14/097ea3241b6af1015239a4a8/statistik-migrasi-penduduk-sulawesi-selatan-hasil-long-form-sensus-penduduk-2024.html",
    # "https://jatim.bps.go.id/publication/2023/07/14/5e64ffbcfb7ac63db40bf9d7/statistik-migrasi-provinsi-jawa-timur-hasil-long-form-sensus-penduduk-2020.html",
    # "https://kaltara.bps.go.id/publication/2023/07/14/d1df267cee7e6ea49cf82d72/statistik-migrasi-kalimantan-utara-hasil-long-form-sensus-penduduk-2020.html",
    # "https://sumbar.bps.go.id/publication/2023/07/14/844319a3740cb2b30e04d183/statistik-migrasi-provinsi-sumatera-barat-hasil-long-form-sensus-penduduk-2020-.html",
    # "https://papua.bps.go.id/publication/2024/02/28/d3863914517dec264cf62df2/provinsi-papua-tengah-dalam-angka-2024.html",
    # "https://papua.bps.go.id/publication/2024/02/28/413572e6ff0362713ffe8bb1/provinsi-papua-selatan-dalam-angka-2024.html",
    # "https://papua.bps.go.id/publication/2023/07/14/04d030076fcdbd463369d3b7/penduduk-provinsi-papua-hasil-long-form-sensus-penduduk-2020.html",
    
]

# os.makedirs("pdf", exist_ok=True)

def run(playwright: Playwright) -> None:
    browser = playwright.chromium.launch(headless=False)
    context = browser.new_context()
    page = context.new_page()
    for idx,url in enumerate(urls):
        print("Url ke : ",idx)
        page.goto(url)
        print(url)
        try:
            page.get_by_role("link", name="").click()
        except Exception as e:
            print(e)    
        try:
            nama = page.query_selector('//*[@id="column2"]/h4')
            nama_dua = nama.inner_text().replace(" ", "_")
        except Exception as e:
            print(e)
        try:
            tanggal = page.query_selector('//*[@id="detail-flipping-publikasi"]/div[2]/div/span[4]')
            tanggal_dua = tanggal.inner_text()
        except Exception as e:
            tanggal_dua = None
            print(e)
        
        try:
            page.query_selector("div#PopTriger").click()
        except Exception as e:
            print(e)
            
        time.sleep(2)
        
        try:
            page.get_by_label("E-Mail:").click()
        except Exception as e:
            print(e)
        time.sleep(2)
        try:
            page.get_by_label("E-Mail:").fill("rakasiwigalih947@gmail.com")
        except Exception as e:
            print(e)
        page.get_by_label("E-Mail:").click()
        unduh = page.query_selector('//*[@id="btn-popup"]/a')
        unduh_link = unduh.get_attribute('href')
        
        link_awal = url.split('/')[2]
        
        unduh_link_asli = "https://"+ link_awal + unduh_link
        
        nama_file = f"{nama_dua}.pdf"
        json_file = f"{nama_dua}.json"

        # Lakukan permintaan untuk mengunduh file
        response = requests.get(unduh_link_asli)

        path_pdf = f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/pdf/{nama_file}"
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
                "update":  tanggal_dua,
                "desc":  "Data statistik",
                "category":  None,
                "sub_category":  None,
                "path_data_raw":  [
                    f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/pdf/{nama_file}",
                    f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{json_file}",
                ],
                "file_name": None,
                "crawling_time_epoch":  int(time.time()),
                "file_path":  None,
                "connection_name":  None,
                "dataset_name":  None,
                "file_path_metadata":  f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{json_file}"
                }
            
            json_s3 = f"s3://ai-pipeline-statistics/data/data_raw/BPS/publikasi/{mapping_name[link_awal.split('.')[0]]}/json/{json_file}"
            json_data = json.dumps(metadata, indent=4, ensure_ascii=False)
            with s3.open(json_s3, 'wb',encoding='utf-8') as s3_file:
                s3_file.write(json_data.encode('utf-8'))
            print('==================================================')
            print(f'File {json_file} berhasil diupload ke S3.')
            print('==================================================')
                
            print(f'Berhasil menyimpan data {json_file}.json')
            
        else:
            print("Gagal mengunduh PDF.")

        # ---------------------
    context.close()
    browser.close()


with sync_playwright() as playwright:
    run(playwright)
