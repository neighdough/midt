"""
python3.5

Currently, there's no handling of timeout errors. To completely finish list of
pages, it's necessary to execute the "restart" method after each timeout error,
passing in a list of visited pages (completed).

Args:
    
Sample implementation

    >>> from scrape_clerk import ScrapeClerk
    >>> sc = ScrapeCleark("2017-01-01", "2018-01-01")
    >>> sc.start()
    >>> ... Timeout Error
    >>> sc.restart(sc.completed)


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
from lxml.html import fromstring

class ScrapeClerk:

    def __init__(self, start_date, end_date):
        """
        Args:
            start_date (str): start date for query in form YYYY-MM-dd
            end_date (str): end date for query in form YYYY-MM-dd
        """

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
        self.completed = []
        self.proxies = self.get_proxies()
        self.proxy = None

    def get_sessid(self, url):
        """
        Generates PHPSESSID

        Args:
            url (str): primary url
        Returns:
            init_req.cookies["PHPSESSID"] (str): session id
        """

        init_req = requests.post(url)
        return init_req.cookies["PHPSESSID"]

    def get_pages(self, url, data):
        """
        Gets a list of pages to be evaluated by running an initial query against
        the website.

        Args:
            url (str): query url established at initialization
            data (dict): a dictionary of parameters to be passed into as part
                of the REST call. Dictionary is created at initialization.
        Returns:
            pages (:list: `int`): list of integers representing the pages 
                containing all of the search results
        """
        req = requests.post(self.url_search, data=data)
        soup = BeautifulSoup(req.text, "html.parser")
        #get list of links to pages containing all results, pull the last one and
        #use that to iterate all results
        page_hyperlinks = soup.find_all("a", {"class": "navigation"})
        pages =[i for i in range(1, int(page_hyperlinks[-1].contents[0]) + 1)]
        return pages

    async def get_table(self, session, page, data):
        """
        Primary method for the class. Sends REST request for specific page
        generated in the get_pages method. Retrieved records are then written
        to a csv on disk.

        Args:
            session (aiohttp.ClientSession): current session 
            page (int): page from the list of pages to be visited
            data (dict): dictionary of payload values to be passed in as part
                of the REST request. Dictionary should contain a PHPSESSID.

        """
        print("Page ", str(page)) 
        with async_timeout.timeout(None):
            params = data
            await asyncio.sleep(.5)
            params["page"] = self.pages.pop(self.pages.index(page))
            async with session.post(self.url_search, data=params, proxy=self.proxy) as response:
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
                await asyncio.sleep(.25)
                self.completed.append(page)
                return await response.release()

    def cancel_tasks(self):
        """
        Cleans up after timeout error
        """

        print("Cancelling tasks")
        asyncio.gather(*asyncio.Task.all_tasks()).cancel()
        self.loop.stop()
        # self.loop.close()

    async def scrape(self, pages, use_proxy=False):
        """
        Initializes the async loop and sends it to the get_table method
        """
        async with aiohttp.ClientSession(loop=self.loop) as session:
            tasks = [self.get_table(session, page, self.params) for page in self.pages]
            await asyncio.gather(*tasks)
    
    def restart(self, completed):
        """
        Restarts async loop in event that it failed for some reason. It takes
        a list of visited pages and then compares that against the full list
        of pages containing all search results, eliminating any page that had
        already been visited.

        Args:
            completed (:list: `int`): list of visited pages
        Returns:
            None
        """

        all_pages = self.get_pages(self.url, self.params)
        self.pages = [p for p in all_pages if p not in self.completed]

        #There seems to be a bug in the aiohttp library that won't allow
        #the use of proxies. In theory, this should speed up the process
        #in the event that the Clerk's website slows down requests after
        #a certain number
        # if self.proxies:
            # self.proxy = "http://"+self.proxies.pop()
        # else:
            # self.proxies = self.get_proxies()
            # self.proxy = "http://"+self.proxies.pop()
        self.loop.run_until_complete(self.scrape(self.loop, True))

    def start(self):
        self.loop.run_until_complete(self.scrape(self.loop))
        
    def get_proxies(self):
        """
        generate list of proxy sites.
        modified from 
        https://www.scrapehero.com/how-to-rotate-proxies-and-ip-addresses-using-python-3/
        """
        url = 'https://us-proxy.org/'
        response = requests.get(url)
        parser = fromstring(response.text)
        proxies = [] 
        for i in parser.xpath('//tbody/tr')[:10]:
            if i.xpath('.//td[7][contains(text(),"no")]'):
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.append(proxy)
        return proxies 
