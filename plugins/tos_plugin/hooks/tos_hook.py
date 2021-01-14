from fusetools.financial_tools import ThinkOrSwim
from airflow.hooks.base_hook import BaseHook
import pandas as pd
import time
import json
import os


class ToSHook(BaseHook):
    def __init__(self):
        self.auth = ThinkOrSwim.authenticate(
            token_path="/home/collier/Dropbox/Skills/Python/Projects/token.pickle",
            api_key=os.environ['tda_api_key'],
            chromedriver_path='/home/collier/Downloads/chromedriver_linux64/chromedriver'
        )

    def pull_quotes(self, ticker, start_date):
        r = ThinkOrSwim.pull_quote_history(
            authentication_object=self.auth,
            ticker=ticker,
            start_date=start_date
        )

        try:
            r = json.loads(r.content.decode('utf8')).get("candles")
        except:
            print("query failed...halting for 10")
            time.sleep(10)
            r = json.loads(r.content.decode('utf8')).get("candles")

        if not r:
            print("invalid api response")
            return

        df = pd.DataFrame({
            "datetime": [x.get("datetime") for x in r],
            "open": [x.get("open") for x in r],
            "close": [x.get("close") for x in r],
            "high": [x.get("high") for x in r],
            "low": [x.get("low") for x in r]
        }).sort_values(by="datetime", ascending=False)

        return df
