import os
from datetime import datetime as dt
import requests
import logging
import time
from tqdm import tqdm
from StockRecord import EodRecord
import traceback

"""
I think the idea is to set the parameters for ALL data sources once in the main.py.
Each of these sources will live in their own file which will contain any helper code
necessary to get the job done.

Tiingo specific symbol adjustments:
Pref stocks (usually hyphenated); "JPM-P-A"; JPM Class A
"." symbols; "BRK-B"; BRK.B
"""

# Set the desired logging resolution here:
logging.basicConfig(filename='logging.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')

# baseUrl = 'https://api.tiingo.com/tiingo/daily/'


def GetData(startDate, endDate, dataFreq, tickers):

    # Initialize our return dictionary
    returnDict = {}

    # Created a counter to serve as the key for the dictionary to be returned to the main program.
    count = 0

    with tqdm(total=len(tickers)) as pbar:
        # for ticker in tqdm(tickers, desc='TiingoData'):
        for ticker in tickers:
            # pass
            if '-' in ticker:
                adjTicker = ticker.replace('-', '-P-')
            elif '.' in ticker:
                adjTicker = ticker.replace('.', '-')
            else:
                adjTicker = ticker

            pbar.update(1)

            # Passing the API key through os.environ.
            headers = {'Content-Type': 'application/json',
                       'Authorization': 'Token ' + os.environ['TIINGO_API_KEY']}

            # Query string specific to this data source's URL.
            # We use adjTicker here to transform our symbol to conform with this specific source's syntax.
            # However, when creating our StockRecord we use OUR ticker so we have a uniform ticker across
            # all sources.
            queryString = f'https://api.tiingo.com/tiingo/daily/{adjTicker}/prices?startDate={startDate}&endDate={endDate}' \
                          f'&format=json&resampleFreq={dataFreq}'

            try:
                jsonResponse = requests.get(queryString, headers=headers)
                responseList = jsonResponse.json()

                # Tiingo's JSON response looks really clean.  I didn't even need the extra step.
                # Left this code here for continuity among the data source helpers.
                # resultsList = responseDict['results']
                resultsList = responseList

                if len(resultsList) != 0:
                    # Iterate through the results and create EodRecord objects for each of results returned.
                    for x in resultsList:
                        rawDate = x['date']
                        convDate = dt.strptime(rawDate, '%Y-%m-%dT%H:%M:%S.%fZ')
                        finalDate = dt.strftime(convDate, '%Y-%m-%d')

                        myRecord = EodRecord(
                            ticker,
                            finalDate,
                            x['open'],
                            x['high'],
                            x['low'],
                            x['close'],
                            x['volume']
                        )

                        returnDict[count] = myRecord
                        count += 1

                else:
                    print(f'Ticker: {ticker}; received an empty JSON response from Tiingo.')

            except:
                print(f'Ticker: {ticker}; failed to get the JSON response from Tiingo.')
                traceback.print_exc()
                logging.INFO(f'Ticker: {ticker}; failed to get the JSON response from Tiingo.')

    return returnDict