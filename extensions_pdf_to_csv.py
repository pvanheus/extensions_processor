import asyncio
import datetime
import email.utils
import os
import os.path
import re
from typing import List, Union
from zipfile import ZipFile

from bs4 import BeautifulSoup
import camelot
import pandas as pd
import requests


def get_valid_filename(s: str) -> str:
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrait_in_2004.jpg'

    from: https://github.com/django/django/blob/master/django/utils/text.py
    """
    s = str(s).strip().replace(" ", "_")
    return re.sub(r"(?u)[^-\w.]", "", s)


def download_main_page() -> List[str]:
    """Downloads the main page and extracts URLs of PDFs to process"""
    page_url = "http://ocpo.treasury.gov.za/Suppliers_Area/Pages/Deviations-and-Exspansions.aspx"
    response = requests.get(page_url)
    if response.status_code == 200:
        soup = BeautifulSoup(response.text, "html.parser")

    report_urls = []
    for div in soup.findAll("div", attrs={"class": "link-item"}):
        report_url = list(div.children)[0].attrs["href"]
        report_urls.append(report_url)
    return report_urls


async def download_pdfs(
    q: asyncio.Queue, report_urls: List[str], cache_dir: str = "pdfs"
) -> List[str]:
    """Downloads PDFs, putting list of PDF paths on q for processing"""
    if not os.path.exists(cache_dir):
        os.mkdir(cache_dir)
    elif not os.path.isdir(cache_dir):
        raise IOError(cache_dir + " exists but is not a directory")
    output_paths = []

    start_epoch = datetime.datetime(1970, 1, 1, 0, 0)
    for url in report_urls:
        response = requests.head(url)
        if response.status_code == 200:
            web_time = int(
                (
                    datetime.datetime(
                        *email.utils.parsedate(response.headers["Last-Modified"])[:6]
                    ) - start_epoch
                ).total_seconds()
            )
            filename = get_valid_filename(url.split("/")[-1])
            output_path = os.path.join(cache_dir, filename)
            disk_time = -1
            if os.path.exists(output_path):
                disk_time = os.stat(output_path).st_mtime
            if disk_time == -1 or disk_time < web_time:
                full_response = requests.get(url)
                if full_response.status_code == 200:
                    with open(output_path, "wb") as output_file:
                        output_file.write(full_response.content)
                    print("downloaded:", output_path)
            else:
                print("skipping download of:", output_path)
            output_paths.append(output_path)
            await q.put(output_path)
    return output_paths


def data_to_csv(pdf_path: str) -> Union[str, ValueError]:
    """Extract table from PDF and convert to CSV

    Return CSV data (as str)"""
    tables = camelot.read_pdf(pdf_path, pages="1-end")
    headers = tables[0].df.iloc[1]
    broken_format = False
    if headers[0].strip() != "Number":
        # affects 'Deviations_-_Quarter_2_2018.pdf' maybe others
        headers[0] = "Number"
        broken_format = True

    try:
        all_data = pd.concat([t.df for t in tables]).iloc[2:]
        if broken_format:
            # drop the last column on the broken 'Deviations_-_Quarter_2_2018.pdf'
            all_data = all_data.iloc[:, :10]
        all_data.columns = headers
    except ValueError as e:
        return e
    else:
        return all_data.to_csv()


async def save_csvs(q: asyncio.Queue, csv_path: str = "csv") -> None:
    """Retrieves PDFs to process from q and saves to disk in csv_path directory"""
    while True:
        if not os.path.exists(csv_path):
            os.mkdir(csv_path)
        elif not os.path.isdir(csv_path):
            raise IOError(csv_path + " exists but is not a directory")
        # print("Waiting on queue")
        pdf_path = await q.get()
        pdf_time = os.stat(pdf_path).st_mtime
        filename = os.path.basename(pdf_path).replace(".pdf", ".csv")
        if "Expantions" in filename:
            filename = filename.replace("Expantions", "Expansions")
        output_path = os.path.join(csv_path, filename)
        csv_time = -1
        if os.path.exists(output_path):
            csv_time = os.stat(output_path).st_mtime
        # print(output_path, "pdf_time", pdf_time, "csv_time", csv_time, "diff", csv_time - pdf_time)
        if csv_time == -1 or csv_time < pdf_time:
            print("processing:", output_path)
            csv_data_maybe = data_to_csv(pdf_path)
            if isinstance(csv_data_maybe, str):  # might be none
                with open(output_path, "w") as output_file:
                    output_file.write(csv_data_maybe)
            else:
                print("Error processing:", output_path, ":", str(csv_data_maybe))
        else:
            print("already processed:", output_path)
        q.task_done()


async def main(ntasks: int, csv_path="csv") -> None:
    """Main routine: downloads main web page, extract PDF urls and processes them using ntasks workers"""
    q = asyncio.Queue()
    report_urls = download_main_page()
    workers = [asyncio.create_task(save_csvs(q, csv_path)) for _ in range(ntasks)]
    await asyncio.gather(download_pdfs(q, report_urls))
    await q.join()
    for worker in workers:
        worker.cancel()
    output_zip = ZipFile("expansions_data.zip", mode="w")
    for filename in os.listdir(csv_path):
        output_zip.write(os.path.join(csv_path, filename))
    output_zip.close()


asyncio.run(main(2))
