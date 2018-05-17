"""
python3.5
TODO:
    incorporate content from 
    https://www.datacamp.com/community/tutorials/asyncio-introduction 
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


url = "https://secure.tncountyclerk.com/"
url_search = url+"businesslist/searchResults.php"


async def get_table(session, page, data):

    #all_rows = []
    #for page in range(1, num_pages + 1):
    print("Page ", str(page)) 
    with async_timeout.timeout(None):
        params = data
        params["page"] = page
        #nxt_req = requests.post(url_search, data=params)
        async with session.post(url_search, data=params) as response:
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

    #await asyncio.sleep(random.random())
    #await queue.put(all_rows)
    #await queue.put(None)

#    msg = "Pulling table for page {}".format(str(page))
#    return msg

async def main(pages):

    init_req = requests.post(url)
    cookie = init_req.cookies
    session = requests.Session()
    data = {
        "BmStartDateSTART_DATE": "2017-01-01",
        "BmStartDateALIAS": "a",
        "BmStartDateEND_DATE": "2018-12-31",
        "orderby": "a.bmBusName",
        "orderbyvalue": "ASC",
        "countylist": "79",
        "token": init_req.cookies["PHPSESSID"]
        }


    req = requests.post(url_search, data=data)
    soup = BeautifulSoup(req.text, "html.parser")
    #get list of links to pages containing all results, pull the last one and
    #use that to iterate all results
    page_hyperlinks = soup.find_all("a", {"class": "navigation"})
    pages = range(1, int(page_hyperlinks[-1].contents[0]) + 1)

    async with aiohttp.ClientSession(loop=loop) as session:
        tasks = [get_table(session, page, data) for page in pages]
        await asyncio.gather(*tasks)

    #full_table = [get_table(page) for page in pages]
    #completed, pending = yield asyncio.wait(full_table)
    #for item in completed:
        #print(item.result())


if __name__ == "__main__":
    loop = asyncio.get_event_loop()
    loop.run_until_complete(main(loop))
   
    # print("Total number of pages: ", len(pages))
    # cols = ["business", "product", "address", "owner", "date"]
    # df = pd.DataFrame(columns = cols)
    # q = Queue()        
    # event_loop = asyncio.get_event_loop()
    # try:
        # event_loop.run_until_complete(main(pages))
    # except:
        # event_loop.close()

