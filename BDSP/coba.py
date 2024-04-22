import os
import re
import asyncio

async def download_and_save(page, s3, subsector, option_satu, option_dua, option_tiga, option_empat):
    async def download_chart(page, label_text, download_text):
        await page.get_by_label(label_text).click()
        async with page.expect_download() as download_info:
            await page.get_by_text(download_text).click(modifiers=["Alt"])
        return await download_info.value

    download_info = await download_chart(page, "View chart menu, Peta Sebaran", "Download XLS")
    download1_info = await download_chart(page, "View chart menu, Produksi dan", "Download XLS")
    download2_info = await download_chart(page, "View chart menu, Kontribusi", "Download XLS")

    downloads = [download_info, download1_info, download2_info]
    download_paths = []

    for idx, download_info in enumerate(downloads):
        download = await download_info.value
        file_path = os.path.join(os.getcwd(), f'temp_download_{idx}.xls')
        await download.save_as(file_path)
        download_paths.append(file_path)

    nama_file = f"{re.sub(r'[^a-zA-Z0-9]+', '_', subsector)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_satu)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_dua)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_tiga)}_{re.sub(r'[^a-zA-Z0-9]+', '_', option_empat)}(1).xls"
    nama_file = nama_file.replace(' ', '_')
    save_path = f"ai-pipeline-statistics/data/data_raw/bdsp_pertanian/data_luas_panen_dan_produksi_komoditas/excel/{nama_file}"

    for idx, download_path in enumerate(download_paths):
        with open(download_path, 'rb') as f:
            s3_path = f"{save_path}_{idx}.xls"
            with s3.open(s3_path, 'wb') as s3_file:
                s3_file.write(f.read())
        os.remove(download_path)

async def main():
    # Assume you have defined 'page' and 's3' objects beforehand
    subsector = "SubsectorName"
    option_satu = "OptionOne"
    option_dua = "OptionTwo"
    option_tiga = "OptionThree"
    option_empat = "OptionFour"
    await download_and_save(page, s3, subsector, option_satu, option_dua, option_tiga, option_empat)

# Run the asyncio event loop
asyncio.run(main())
