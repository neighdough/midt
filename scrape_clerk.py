from bs4 import BeautifulSoup
import getopt
import json
import numpy as np
import pandas as pd
import requests


url = "https://secure.tncountyclerk.com/"
init_req = requests.post(url)
cookie = init_req.cookies
session = requests.Session()


#token: e12721132646fa51294788fb62a88958
data = {
    "BmStartDateSTART_DATE": "2017-01-01",
    "BmStartDateALIAS": "a",
    "BmStartDateEND_DATE": "2017-12-31",
    "orderby": "a.bmBusName",
    "orderbyvalue": "ASC",
    "countylist": "79",
    "token": init_req.cookies["PHPSESSID"]}

url_search = url+"businesslist/searchResults.php"
req = requests.post(url_search, data=data)
soup = BeautifulSoup(req.text, "html5lib")
#get list of links to pages containing all results, pull the last one and
#use that to iterate all results
page_hyperlinks = soup.find_all("a", {"class": "navigation"})
num_pages = int(page_hyperlinks[-1].contents[0])

all_rows = []
for page in range(1, num_pages + 1):
    print page
    data["page"] = page
    nxt_req = requests.post(url_search, data=data)
    nxt_soup = BeautifulSoup(nxt_req.text, "html5lib")
    table = nxt_soup.find(text="Business Name").find_parent("table")
    for row in table.find_all("tr"):
        cur_row = [cell.get_text(strip=True) for cell in row.find_all("td")]
        if len(cur_row) == 5:
            all_rows.append(cur_row)


