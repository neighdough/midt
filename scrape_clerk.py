"""
python3.5
TODO:
    - Handle timeout error
    - Attempt to implement multiprocessing

"""
import asyncio
from asyncio import Queue
import async_timeout
from bs4 import BeautifulSoup
import getopt
import json
import numpy as np
import pandas as pd
import requests
import random
import aiohttp
import concurrent


class ScrapeClerk:

    def __init__(self, start_date, end_date):
        self.start_date = start_date
        self.end_date = end_date
        self.url = "https://secure.tncountyclerk.com/"
        self.url_search = ("https://secure.tncountyclerk.com/"
                            "businesslist/searchResults.php")
        self.sessid = self.get_sessid(self.url)
        self.params = {
                    "BmStartDateSTART_DATE": self.start_date,
                    "BmStartDateALIAS": "a",
                    "BmStartDateEND_DATE": self.end_date,
                    "orderby": "a.bmBusName",
                    "orderbyvalue": "ASC",
                    "countylist": "79",
                    "token": self.sessid
                    }
        self.pages = self.get_pages(self.url_search, self.params)
        self.loop = asyncio.get_event_loop()

    def get_sessid(self, url):
        init_req = requests.post(url)
        return init_req.cookies["PHPSESSID"]

    def get_pages(self, url, data):
        req = requests.post(self.url_search, data=data)
        soup = BeautifulSoup(req.text, "html.parser")
        #get list of links to pages containing all results, pull the last one and
        #use that to iterate all results
        page_hyperlinks = soup.find_all("a", {"class": "navigation"})
        #pages = asyncio.Queue(loop=self.loop)
        pages =[i for i in range(1, int(page_hyperlinks[-1].contents[0]) + 1)]
        return pages

    async def get_table(self, session, page, data):
        print("Page ", str(page)) 
        with async_timeout.timeout(20):
            params = data
            await asyncio.sleep(.5)
            params["page"] = self.pages.pop(self.pages.index(page))
            async with session.post(self.url_search, data=params) as response:
                txt = await response.read()
                nxt_soup = BeautifulSoup(txt, "lxml")
                table = nxt_soup.find(text="Business Name").find_parent("table")
                all_rows = []
                for row in table.find_all("tr"):
                    cur_row = [cell.get_text(strip=True) for cell in row.find_all("td")]
                    if len(cur_row) == 5:
                        all_rows.append(cur_row)
                cols = ["business", "product", "address", "owner", "date"] 
                df = pd.DataFrame(all_rows, columns=cols)
                with open('./buslic.csv', 'a') as f_handle:
                    df.to_csv(f_handle, header=False)
                return await response.release()

    def cancel_tasks(self):
        print("Cancelling tasks")
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        self.loop.stop()
        # self.loop.close()

    async def scrape(self, pages):

        async with aiohttp.ClientSession(loop=self.loop) as session:
            tasks = [self.get_table(session, page, self.params) for page in self.pages]
            await asyncio.gather(*tasks)

    def start(self):
        try:
            self.loop.run_until_complete(self.scrape(self.loop))
        except :#asyncio.TimeoutError:
            print("------------------/nTimeout, reconnecting")
            self.cancel_tasks()
            self.sessid = self.get_sessid(self.url)
            self.params["token"] = self.sessid
            #self.get_table(session, page, params)
            self.loop = asyncio.new_event_loop()
            asyncio.set_event_loop(asyncio.new_event_loop())
            self.loop = asyncio.get_event_loop()
            self.loop.run_until_complete(self.scrape(self.loop))

