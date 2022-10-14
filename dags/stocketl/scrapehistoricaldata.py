import scrapy
from scrapy.crawler import CrawlerProcess
from datetime import datetime, timedelta
import csv
from urllib.parse import urlencode
from scrapy.utils.project import get_project_settings
import asyncio
import os
from database import Database


class MoneyControlSpider(scrapy.Spider):
    name = "moneycontrol-web"
    chunk_size = 8
    data = []
    format_str = '%Y-%m-%d'
    custom_settings = {
        "DOWNLOAD_DELAY": 3,
        'DOWNLOADER_MIDDLEWARES':  {
            'scrapy.downloadermiddlewares.cookies.CookiesMiddleware': 700
        },
        'COOKIES_ENABLED': True,
        "DOWNLOAD_DELAY": 3,

    }
    headers = {
        'authority': 'www.moneycontrol.com',
        'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9',
        'accept-language': 'en-US,en;q=0.9,pt;q=0.8',
        'cache-control': 'no-cache',
        'content-type': 'application/x-www-form-urlencoded',
        'origin': 'https://www.moneycontrol.com',
        'pragma': 'no-cache',
        'sec-fetch-dest': 'document',
        'sec-fetch-mode': 'navigate',
        'sec-fetch-site': 'same-origin',
        'sec-fetch-user': '?1',
        'upgrade-insecure-requests': '1',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36'
    }

    def __init__(self, stock_id=None, stock_code=None, stock_name=None, from_date="01-01-2000"):
        self.input = input  # source file name
        self.stock_id = stock_id
        self.stock_code = stock_code
        self.stock_name = stock_name
        self.from_date = from_date

    def start_requests(self):

        # url = "https://www.moneycontrol.com/stocks/hist_stock_result.php?ex=B&sc_id=H&mycomp=Hindalco%20Industries"
        now = datetime.now()
        yesterday = now - timedelta(days=1)
        self.from_date = self.from_date + timedelta(days=1)
        data = {
            "x": str(now.hour),
            "y": str(now.minute),
            "frm_dy": str(self.from_date.day),
            "frm_mth": str(self.from_date.month),
            "frm_yr": str(self.from_date.year),
            "to_dy": str(yesterday.day),
            "to_mth": str(yesterday.month),
            "to_yr": str(yesterday.year),
            "hdn": "daily"
        }
        params = {
            "ex": "B",
            "sc_id": self.stock_code,
            "mycomp": self.stock_name
        }
        url = "https://www.moneycontrol.com/stocks/hist_stock_result.php?" + \
            urlencode(params)

        if url:
            yield scrapy.FormRequest(url,
                                     formdata=data,
                                     callback=self.parse, headers=self.headers)

    def parse(self, response):
        datas = response.css("table.tblchart tr td::text").extract()
        for i in range(0, len(datas), self.chunk_size):
            yield {
                "stock_id": self.stock_id,
                "ondate": datas[i],
                "openat": datas[(i+1)],
                "high": datas[(i+2)],
                "low": datas[(i+3)],
                "closeat": datas[(i+4)],
                "volume": datas[(i+5)],
                "hl": datas[(i+6)],
                "oc": datas[(i+7)]
            }
        if response.css("a.nextprev"):
            url = response.url.split("?")[0]
            url = url + response.css("a.nextprev::attr(href)").extract_first()
            yield scrapy.Request(url,
                                 callback=self.parse, headers=self.headers, meta=response.meta)


async def main():
    path = "dumpdata"
    if not os.path.exists(path):
        os.makedirs(path)
    con = Database.getConnection()
    cur = con.cursor()
    cur.execute(
        "SELECT id,stock_name,stock_code,stock_value_latest_date FROM stock_details")

    rows = cur.fetchall()

    for row in rows:
        from_date = row[3]
        s = get_project_settings()
        process = CrawlerProcess(s)
        s.update(
            {
                'FEEDS': {
                    path+"/"+row[2]+'_data.csv': {
                    # row[2]+'_data.csv': {
                        'format': 'csv',
                        'overwrite': True
                    }
                },
                'CONCURRENT_REQUESTS_PER_DOMAIN': 10,
                'CONCURRENT_REQUESTS': 5
            }
        )
        process.crawl(MoneyControlSpider, row[0], row[2], row[1], from_date)
        # break
    process.start()  # the script will block here until the crawling is finished

asyncio.run(main())
