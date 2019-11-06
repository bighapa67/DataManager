from datetime import datetime as dt
import pandas as pd
import traceback
from StockRecord import EodRecord
import logging
from tqdm import tqdm

'''
Need to decide how I want to filter the EOD exchange updates for just the symbols we care about before DB insertion.

EODData does not offer an API.  You have to download their 'app' which automatically downloads the daily data
from their server.  The app allows you to set the path where EOD will deliver the csv files for the exchanges
you specified within the app.

Apparently EOD doesn't include volume?  Filling it in with zero for now in my stock record object.
'''


def GetData(startDate, endDate, tickers):

    # pbar = tqdm(len(tickers))

    try:
        # Need to add the same file path validity check until we decide on a cloud location for all
        # our files.
        # Will likely do three of these data frames (one for each exchange) and then combine them.
        eod_df = pd.read_csv('C:\\Users\\kjone\\Google Drive\\StockOdds\\AMEX_20191105_test.csv')

        # pbar = tqdm(eod_df.count())
        returnDict = {}

        for index, row in eod_df.iterrows():
            # pbar.update(1)

            rawDate = row['Date']
            convDate = dt.strptime(rawDate, '%d-%b-%Y')
            finalDate = dt.strftime(convDate, '%Y-%m-%d')

            myRecord = EodRecord(
                row['Symbol'],
                finalDate,
                row['Open'],
                row['High'],
                row['Low'],
                row['Close'],
                0,
            )

            returnDict[index] = myRecord

        return returnDict

    except:
        traceback.print_exc()

