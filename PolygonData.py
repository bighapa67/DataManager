import os
import requests
import logging
from datetime import datetime as dt
from StockRecord import EodRecord
import traceback
from tqdm import tqdm

"""
Trying to split the specifics of the data sources from non-request specific code.
"""
queryCounter = 1

# Set the desired logging resolution here:
logging.basicConfig(filename='logging.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')


def GetData(startDate, endDate, tickers):

    pbar = tqdm(total=len(tickers))

    returnDict = {}
    count = 0

    for ticker in tickers:
        pbar.update(1)

        queryString = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}?' \
                      f'unadjusted=false&apiKey=' + os.environ['POLYGON_API_KEY']

        try:
            jsonResponse = requests.get(queryString)
            responseDict = jsonResponse.json()
            responseList = responseDict['results']

            for x in responseList:
                openPx = x['o']
                highPx = x['h']
                lowPx = x['l']
                closePx = x['c']
                volume = x['v']
                trueRange = abs(highPx - lowPx)
                rawDate = x['t']

                # This almost caused a HUGE problem.  The basic datetime.fromtimestamp
                convDate = dt.utcfromtimestamp(rawDate / 1000).strftime('%Y-%m-%d')

                myRecord = EodRecord(
                    ticker,
                    convDate,
                    x['o'],
                    x['h'],
                    x['l'],
                    x['c'],
                    x['v']
                )

                returnDict[count] = myRecord
                count += 1

        except:
            traceback.print_exc()
            print(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')
            logging.INFO(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')

    return returnDict