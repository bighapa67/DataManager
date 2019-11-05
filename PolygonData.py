import os
import requests
import logging
from datetime import datetime as dt
from StockRecord import EodRecord

"""
Trying to split the specifics of the data sources from non-request specific code.
"""
queryCounter = 1

# Set the desired logging resolution here:
logging.basicConfig(filename='logging.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')


def GetData(startDate, endDate, tickers):

    # pbar = tqdm(total=len(tickers))

    for ticker in tickers:
        # pbar.update(1)

        returnDict = {}

        queryString = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}?' \
                      f'unadjusted=false&apiKey=' + os.environ['POLYGON_API_KEY']

        try:
            i = 0
            jsonResponse = requests.get(queryString)
            responseDict = jsonResponse.json()

            # Iterate through the results and create EodRecord objects for each of results returned.
            # for x in responseDict:
            for key, value in responseDict.items():
                rawData = value['results'[0]]
                # rawDate = x['t']
                convDate = dt.utcfromtimestamp(rawDate / 1000).strftime('%Y-%m-%d')
                # convDate = dt.strptime(rawDate, '%Y-%m-%dT%H:%M:%S.%fZ')
                # finalDate = dt.strftime(convDate, '%Y-%m-%d')

                myRecord = EodRecord(
                    convDate,
                    x['o'],
                    x['h'],
                    x['l'],
                    x['c'],
                    x['v']
                )

                returnDict[i] = myRecord
                i += 1

            return returnDict

        except:
            print(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')
            logging.INFO(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')
        finally:
            ticker = str(responseDict['ticker'])
            resultsList = responseDict['results']
            return  resultsList


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
