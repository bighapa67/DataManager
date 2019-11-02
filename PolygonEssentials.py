import os
import requests
import logging

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

        # queryString = (histRangeUrl + ticker + '/range/1/day/' + startDate + '/' + endDate + '?unadjusted='
        #                + unadjusted + '&apiKey=' + os.environ['POLYGON_API_KEY'])

        queryString = f'https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{startDate}/{endDate}?' \
                      f'apiKey=' + os.environ['POLYGON_API_KEY']

        try:
            jsonResponse = requests.get(queryString)
            responseDict = jsonResponse.json()
        except:
            print(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')
            logging.INFO(f'Ticker: {ticker}; failed to get the JSON response from Polygon.')
        finally:
            ticker = str(responseDict['ticker'])
            resultsDict = responseDict['results']
            return  resultsDict


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
