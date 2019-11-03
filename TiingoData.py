import os
import requests
import logging
import time
from tqdm import tqdm
from StockRecord import EodRecord as rec

"""
I think the idea is to set the parameters for ALL data sources once in the main.py.
Each of these sources will live in their own file which will contain any helper code
necessary to get the job done.

Parameters needed:
API Key (set as an os.environ so not needed)
Start Date
End Date
Tickers Array []

"""

# Set the desired logging resolution here:
logging.basicConfig(filename='logging.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')

# baseUrl = 'https://api.tiingo.com/tiingo/daily/'


def GetData(startDate, endDate, dataFreq, tickers):

    # pbar = tqdm(total=len(tickers))

    for ticker in tickers:
        # pbar.update(1)

        # Passing the API key through os.environ.
        headers = {'Content-Type': 'application/json',
                   'Authorization': 'Token ' + os.environ['TIINGO_API_KEY']}

        # Query string specific to this data source's URL.
        queryString = f'https://api.tiingo.com/tiingo/daily/{ticker}/prices?startDate={startDate}&endDate={endDate}' \
                      f'&format=json&resampleFreq={dataFreq}'

        try:
            jsonResponse = requests.get(queryString, headers=headers)
            responseDict = jsonResponse.json()
            i = 0

            # Tiingo's JSON response looks really clean.  I didn't even need the extra step.
            # resultsDict = responseDict['results']
            resultsDict = responseDict
            returnDict[] =

            # This needs to convert ALL the records in the resultsDict.
            # Right now I'm just writing over the first record over and over.
            for x in resultsDict:
                record = {'openPx': x['open'],
                              'highPx': x['high'],
                              'lowPx': x['low'],
                              'closePx': x['close'],
                              'volume': x['volume'],
                              'rawDate': x['date']}
                # returnDict = {i: record}
                returnDict[i] = record
                i+=1

            return returnDict
        except:
            print(f'Ticker: {ticker}; failed to get the JSON response from Tiingo.')
            logging.INFO(f'Ticker: {ticker}; failed to get the JSON response from Tiingo.')

            # Lol... I doubt this is correct...
            return f'{ticker} failed to get a JSON response from Tiingo.'


# for x in resultsDict:
#     openPx = x['o']
#     highPx = x['h']
#     lowPx = x['l']
#     closePx = x['c']
#     volume = x['v']
#     trueRange = abs(highPx - lowPx)
#     rawDate = x['t']
#     # This almost caused a HUGE problem.  The basic datetime.fromtimestamp apparently returns the local time
#     # of the machine
#     convDate = dt.datetime.utcfromtimestamp(rawDate / 1000).strftime('%Y-%m-%d')
