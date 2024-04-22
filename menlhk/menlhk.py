# from datetime import datetime
# import json
# import os
# import requests
# from requests import Session

# requests: Session = Session()

# requests.headers.update({
#     'authority': 'geocode.arcgis.com',
#     'accept': '*/*',
#     'accept-language': 'id-ID,id;q=0.9,en-US;q=0.8,en;q=0.7',
#     'if-none-match': '"427af446"',
#     'origin': 'https://geoportal.menlhk.go.id',
#     'referer': 'https://geoportal.menlhk.go.id/',
#     'sec-ch-ua': '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
#     'sec-ch-ua-mobile': '?0',
#     'sec-ch-ua-platform': '"Windows"',
#     'sec-fetch-dest': 'empty',
#     'sec-fetch-mode': 'cors',
#     'sec-fetch-site': 'cross-site',
#     'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
# })


# key = [
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM4OTYzMjYjbG5nPTk4I3BsPTMzODE2NDE5I2xicz0xNDoyNDY0Mjc5NyNsbj1Xb3JsZA==',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQyNzczNDYjbG5nPTk4I3BsPTM0NzI1MjgyI2xicz0xNDoyNDY0MzAxNTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQxNTUwOTYjbG5nPTk4I3BsPTM0MzYxMTE5I2xicz0xNDoyNDY0MzAxMjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM1ODkzMDUjbG5nPTk4I3BsPTMzMjU0MDMzI2xicz0xNDo4NjgyMTMwOzA6MjMzOTIxODYjbG49V29ybGQ=',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ2NTI5ODQjbG5nPTUzI3BsPTM1OTc1NDk0I2xicz0xNDoyNTAxODY3MDswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM3MTI5ODQjbG5nPTk4I3BsPTMzNTAxNDY5I2xicz0xNDoyNDQ0NDA3NjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1OTE3NDUjbG5nPTk4I3BsPTM1Nzg5NTM2I2xicz0xNDo0NzczNzcxMCNsbj1Xb3JsZA==',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1ODEyMTYjbG5nPTk4I3BsPTM1NzIzODU3I2xicz0xNDo0NzczNzY2NSNsbj1Xb3JsZA==',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ2MTI3MzUjbG5nPTk4I3BsPTM1ODQ4MTI0I2xicz0xNDo0NzczNzcyMTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1MDAzMTkjbG5nPTk4I3BsPTM1NDk0ODMyI2xicz0xNDo0MTQ4ODg1MjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NDUwODQjbG5nPTk4I3BsPTM1MzEzMTEzI2xicz0xNDoyNjQ0NDQ0MyNsbj1Xb3JsZA==',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM4ODkzOTgjbG5nPTk4I3BsPTMzNzg3MjY4I2xicz0xNDoyNDQ3NjMxMzswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM2ODg0MzgjbG5nPTk4I3BsPTMzNDAyNzY3I2xicz0xNDo5MjY5OTcwOzA6MjMzOTIxODYjbG49V29ybGQ=',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NTAxMTkjbG5nPTk4I3BsPTM1MzU2MTc1I2xicz0xNDoyODc5NjY0NzswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NDE4MzMjbG5nPTk4I3BsPTM1Mjk1OTcwI2xicz0xNDo4NjA2MDQwOzA6MjMzOTIxODYjbG49V29ybGQ=',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0MDI0OTMjbG5nPTk4I3BsPTM1MTYyMzk2I2xicz0xNDoyNTY1MDY2OCNsbj1Xb3JsZA==',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0MTMyNjUjbG5nPTk4I3BsPTM1MTk1MDk4I2xicz0xNDoyNTY1MDY5MjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0MzIyMDEjbG5nPTk4I3BsPTM1MjUxODcyI2xicz0xNDoyNTY1MDY5NjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0MjUwNzYjbG5nPTk4I3BsPTM1MjM0NDMzI2xicz0xNDoyNTY1MDY5NTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NDA4MzkjbG5nPTk4I3BsPTM1MjkxMjcyI2xicz0xNDoyNTY1MDY5ODswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1Njk5NjYjbG5nPTk4I3BsPTM1Njk5NDU5I2xicz0xNDo0NzcxMzAyMjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1NDg2MTQjbG5nPTk4I3BsPTM1NjUwMTUwI2xicz0xNDo0NzcxMzAxODswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1MTgyNTYjbG5nPTk4I3BsPTM1NTYwMDYwI2xicz0xNDo0NzcxMzAxNTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1NTkwODIjbG5nPTk4I3BsPTM1Njc3NDEwI2xicz0xNDo0NzcxMzAxOTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDM4ODMzNzcjbG5nPTk4I3BsPTMzNzcxNzgyI2xicz0xNDoyMDgwNjYxOTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ1MTM4OTcjbG5nPTk4I3BsPTM1NTUxMzUzI2xicz0xNDo0NzcxMjk5NTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0OTE1NDgjbG5nPTk4I3BsPTM1NDc0NTk0I2xicz0xNDozNzIzNjUyMTswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0OTQ4MTQjbG5nPTk4I3BsPTM1NDgxODg2I2xicz0xNDozNzIzNjUyNjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NzAzMDEjbG5nPTk4I3BsPTM1NDE2NTYzI2xicz0xNDozMTA1NTI2NzswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0NzM5MTUjbG5nPTk4I3BsPTM1NDI0Nzk0I2xicz0xNDozMTA1NTI4MjswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0Nzc0ODcjbG5nPTk4I3BsPTM1NDMyNzc5I2xicz0xNDozNTY1NDM1ODswOjIzMzkyMTg2I2xuPVdvcmxk',
#         'dHA9MCN0dj02NTc3YTY2NiNsb2M9NDQ0ODM1NzEjbG5nPTk4I3BsPTM1NDU3ODQzI2xicz0xNDozNTY1NDM2MDswOjIzMzkyMTg2I2xuPVdvcmxk'
#     ]

# request_successful = False

# for magickey in key:
#     # If the request is successful, no need to continue the loop
#     if request_successful:
#         break

#     response = requests.get(f'https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer/findAddressCandidates?SingleLine=Jawa%20Barat%2C%20IDN&f=json&outSR=%7B%22wkid%22%3A102100%7D&outFields=*&magicKey={magickey}&maxLocations=6')
    

#     # If the response is successful, print the data and set request_successful to True
#     if response.status_code == 200:
#         data = response.json()
#         if 'candidates' in data and len(data['candidates']) > 0:
#             extent = data['candidates'][0]['extent']
#             atribute = data['candidates'][0]['attributes']
            
#             xmin = extent['xmin']
#             ymin = extent['ymin']
#             xmax = extent['xmax']
#             ymax = extent['ymax']
            
#             # print(f'xmin: {xmin}, ymin: {ymin}, xmax: {xmax}, ymax: {ymax}')
            
#             response_data = requests.get(f'https://geoportal.menlhk.go.id/server/rest/services/SIGAP_Interaktif/Kawasan_Hutan/MapServer/0/query?f=json&returnGeometry=true&spatialRel=esriSpatialRelIntersects&geometry=%7B%22xmin%22%3A{xmin}%2C%22ymin%22%3A{ymin}%2C%22xmax%22%3A{xmax}%2C%22ymax%22%3A{ymax}%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%7D%7D&geometryType=esriGeometryEnvelope&inSR=102100&outFields=OBJECTID%2CKODE_PROV%2CFUNGSIKWS%2CNOSKKWS%2CTGLSKKWS%2CNAMOBJ%2CREMARK&outSR=102100&quantizationParameters=%7B%22mode%22%3A%22view%22%2C%22originPosition%22%3A%22upperLeft%22%2C%22tolerance%22%3A38.21851414258623%2C%22extent%22%3A%7B%22xmin%22%3A10572229.315355161%2C%22ymin%22%3A-1232970.4198342145%2C%22xmax%22%3A15698279.243597647%2C%22ymax%22%3A677752.6509227199%2C%22spatialReference%22%3A%7B%22wkid%22%3A102100%2C%22latestWkid%22%3A3857%7D%7D%7D')
            
#             data_full = response_data.json()
            
#             kode_prov = data_full['features'][0]['attributes']['KODE_PROV']
            
            
#             metadata = {
#                 'link':'https://geoportal.menlhk.go.id/Interaktif2/',
#                 'domain':'geoportal.menlhk.go.id',
#                 'tag':[
#                     'geoportal',
#                     'menlhk',
#                     'Peta Interaktif'
#                 ],
#                 'kode_prov':kode_prov,
#                 'prov_name':atribute['Match_addr'],
#                 'fungsi_kawasan_hutan':data_full['features'][0]['attributes']['FUNGSIKWS'],
#                 'no_sk_kawasan_hutan':data_full['features'][0]['attributes']['NOSKKWS'],
#                 'tanggal_sk_kawasan_hutan':data_full['features'][0]['attributes']['TGLSKKWS'],
#                 'longitude':atribute['X'],
#                 'latitude':atribute['Y'],
#                 'crawling_time':datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
#                 'crawling_time_epoch':datetime.now().timestamp(),
#             }
            
#             file_json = metadata['prov_name'].replace(' ','_')

#             os.makedirs('menlhk', exist_ok=True)
#             with open(f'menlhk/{file_json}.json', 'w', encoding='utf-8') as f:
#                 json.dump(metadata, f, ensure_ascii=False, indent=4)
                
#             print(f'Berhasil menyimpan data {file_json}.json')
#     else:
#         print(f"Request failed for magic key {magickey}")

# # Print a message if no successful request is found
# if not request_successful:
#     print("No successful request found")


province_mapping = {
    11: "Aceh",
    12: "Sumatera Utara",
    13: "Sumatera Barat",
    16: "Sumatera Selatan",
    14: "Riau",
    21: "Kepulauan Riau",
    15: "Jambi",
    17: "Bengkulu",
    18: "Lampung",
    19: "Bangka Belitung",
    31: "DKI Jakarta",
    36: "Banten",
    32: "Jawa Barat",
    33: "Jawa Tengah",
    34: "DI Yogyakarta",
    35: "Jawa Timur",
    51: "Bali",
    61: "Kalimantan Barat",
    62: "Kalimantan Tengah",
    63: "Kalimantan Selatan",
    64: "Kalimantan Timur",
    65: "Kalimantan Utara",
    52: "Nusa Tenggara Barat",
    53: "Nusa Tenggara Timur",
    71: "Sulawesi Utara",
    72: "Sulawesi Tengah",
    73: "Sulawesi Selatan",
    74: "Sulawesi Tenggara",
    75: "Gorontalo",
    76: "Sulawesi Barat",
    81: "Maluku",
    82: "Maluku Utara",
    91: "Papua",
    92: "Papua Barat"
}

# Contoh penggunaan:
angka = 11
print(province_mapping.get(angka))