import os
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
    filename = '19_11_07_composite.csv'
    count = 0
    eodd_pbar = tqdm(total=len(tickers), desc='EODData')

    try:
        # Need to add the same file path validity check until we decide on a cloud location for all
        # our files.
        # Will likely do three of these data frames (one for each exchange) and then combine them.
        # Going to need to add a mechanism to figure out which file name (date really) we're looking for
        # for each of the three exchanges along with a way for the program to automatically handle a situation where
        # we normally process the files at say 5:00pm CT, but for some reason the files aren't available.  We
        # may then have to access the files after midnight (i.e. T+1 or more).
        if os.path.exists(f'C:\\Users\\kjone\\Google Drive\\StockOdds\\EODData\\{filename}'):
            eod_df = pd.read_csv(f'C:\\Users\\kjone\\Google Drive\\StockOdds\\EODData\\{filename}')
        elif os.path.exists(f'C:\\Users\\Ken\\Google Drive\\StockOdds\\EODData\\{filename}'):
            eod_df = pd.read_csv(f'C:\\Users\\Ken\\Google Drive\\StockOdds\\EODData\\{filename}')

        returnDict = {}
        data_filter = eod_df['Symbol']

        # for index, row in eod_df.iterrows():
            # eodd_pbar.update(1)
        for ticker in tickers:
            eodd_pbar.update(1)

            if ticker in data_filter.values:

                dataRow = eod_df[eod_df['Symbol'] == ticker]

                # Since EODData auto-sends up csv files for our exchanges (currently AMEX, NASDAQ, NYSE), we need
                # to filter the data for only the symbols we're interested in.
                rawDate = dataRow['Date'].values
                # tempVar = rawDate[0]
                tempVar = '7-Nov-19'
                convDate = dt.strptime(tempVar, '%d-%b-%y')
                finalDate = dt.strftime(convDate, '%Y-%m-%d')

                myRecord = EodRecord(
                        dataRow['Symbol'].values[0],
                        finalDate,
                        dataRow['Open'].values[0],
                        dataRow['High'].values[0],
                        dataRow['Low'].values[0],
                        dataRow['Close'].values[0],
                        0,
                    )
                returnDict[count] = myRecord
                count += 1
            else:
                with open('C:\\Users\\Public\\Documents\\EODDataSymbolErrors.txt', 'a') as f:
                    f.write(f'{ticker}\n')

                continue

        return returnDict

    except:
        traceback.print_exc()
        with open('C:\\Users\\Public\\Documents\\EODDataSymbolErrors.txt', 'a') as f:
            f.write(f'{ticker}\n')


