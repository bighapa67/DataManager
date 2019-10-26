import json
import requests
import os
import datetime as dt

os.environ['API_KEY'] = 'Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t'
histRangeUrl = 'https://api.polygon.io/v2/aggs/ticker/'
symbol = 'AAPL'
startDate = '2019-01-01'
endDate = '2019-02-01'
unadjusted = 'false'

queryString = (histRangeUrl + symbol + '/range/1/day/' + startDate + '/' + endDate + '?unadjusted=' \
                            + unadjusted + '&apiKey=' + os.environ['API_KEY'])

jsonResponse = requests.get(queryString)
responseDict = jsonResponse.json()

#Nice simple explanation of how to handle nested dictionary JSON responses:
#https://stackoverflow.com/questions/51788550/parsing-json-nested-dictionary-using-python
ticker = responseDict['ticker']
resultsDict = responseDict['results']
for x in resultsDict:
    openPx = x['o']
    highPx = x['h']
    lowPx = x['l']
    closePx = x['c']
    volume = x['v']
    rawDate = x['t']
    convDate = dt.datetime.fromtimestamp(rawDate / 1000).strftime('%Y-%m-%d')

    print('Ticker: ' + str(ticker))
    print('Open: ' + str(openPx))
    print('High: ' + str(highPx))
    print('Low: ' + str(lowPx))
    print('Close: ' + str(closePx))
    print('Volume: ' + str(volume))
    print('Date: ' + str(convDate))