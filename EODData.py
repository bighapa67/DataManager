from datetime import datetime as dt
import pandas as pd
import traceback
from StockRecord import EodRecord
import logging

'''
EODData does not offer an API.  You have to download their 'app' which automatically downloads the daily data
from their server.  The app allows you to set the path where EOD will deliver the csv files for the exchanges
you specified within the app.
'''

records = []

def GetData(startDate, endDate, tickers):
    try:
        eod_df = pd.read_csv('C:\\Users\\kjone\\Google Drive\\StockOdds\\AMEX_20191105_test.csv')

        for index, row in eod_df.iterrows():
            record = index
            records.append(record)

    except:
        traceback.print_exc()

