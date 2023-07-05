from pydantic import BaseModel
from typing import List, Optional
import datetime
import requests
import os
import json


class Ticker(BaseModel):
    ticker: str             # A
    name: str               # Agilent Technologies Inc
    market: str             # stocks
    locale: str             # us
    primary_exchange: Optional[str]   # XNYS
    active : bool           # true/false
    currency_name: str      # usd
    cik: Optional[str]                # 00001231
    composite_figi: Optional[str]     # BBG000000
    share_class_figi: Optional[str]   # BBG000000
    last_updated_utc: datetime.datetime # "2023-05-04T05:01:27.776Z"

class TickerListResponse(BaseModel):
    results: List[Ticker]   # [Ticker Reference objects]
    status: str             # OK
    request_id: str         # w12345
    count: int              # 1000 (which is max)
    next_url: Optional[str] # "https://...." without the api key


class TickerListRequest:
    def __init__(self,api_key:str,page_num:int,cursor,outdir):
        self.cursor=cursor
        self.api_key = api_key
        self.page_num = page_num
        self.limit = 1000
        self.endpoint = "https://api.polygon.io"
        if cursor:
            self.query = cursor + f"&limit={1000}&apiKey={api_key}"
        else:
            self.query = self.endpoint + f"/v3/reference/tickers?limit={1000}&apiKey={api_key}"

        self.response = requests.get(self.query)
        # saving response
        self.data = self.response.json()
        with open(os.path.join(outdir,f"response_page{page_num}.json"),"w") as outfile:
            json.dump(self.data,outfile,indent=4)
        with open(os.path.join(outdir,f"ticker_page{page_num}.json"),"w") as outfile:
            json.dump(self.data["results"],outfile,indent=4)

    def get_next_cursor(self):
        # if there is a next page, return
        if "next_url" in self.data:
            return self.data["next_url"]
        else:
            return None

    def validate(self):
        pass

class GroupedDailyRequest:
    def __init__(self,date,api_key:str,outdir):
        self.api_key = api_key
        self.endpoint = "https://api.polygon.io"
        self.query = self.endpoint + f"/v2/aggs/grouped/locale/us/market/stocks/{date}?include_otc=true&apiKey={api_key}"

        try:
            self.response = requests.get(self.query)
            # saving response
            self.data = self.response.json()
            with open(os.path.join(outdir,f"{date}_full.json"),"w") as outfile:
                json.dump(self.data,outfile,indent=4)
            with open(os.path.join(outdir,f"{date}.json"),"w") as outfile:
                if self.data["resultsCount"] != 0:
                    json.dump(self.data["results"],outfile,indent=4)
                else:
                    json.dump([{}],outfile,indent=4)

        except Exception as err:
            print(err)





