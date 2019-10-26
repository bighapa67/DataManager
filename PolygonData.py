import json
import requests
import os
import datetime as dt
import MySQLdb as sqldb

os.environ['API_KEY'] = 'Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t'
os.environ['DB_USER'] = 'bighapa67'
os.environ['DB_PSWD'] = 'kando1'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'pythondb'

histRangeUrl = 'https://api.polygon.io/v2/aggs/ticker/'
# symbol will need to become a loop over a .csv symbol list.
symbol = 'AAPL'
startDate = '2019-01-01'
endDate = '2019-02-01'
unadjusted = 'false'

queryString = (histRangeUrl + symbol + '/range/1/day/' + startDate + '/' + endDate + '?unadjusted='
                            + unadjusted + '&apiKey=' + os.environ['API_KEY'])

# dbConnect = sqldb.connect(user=os.environ(['DB_USER']),
#                           password=os.environ(['DB_PSWD']),
#                           host=os.environ(['DB_HOST']),
#                           database=os.environ(['DB_NAME']),
#                           use_unicode=True,
#                           charset='utf8'
#                           )

dbConnect = sqldb.connect(user='bighapa67',
                          password='kando1',
                          host='localhost',
                          database='pythondb',
                          use_unicode=True,
                          charset='utf8'
                          )

jsonResponse = requests.get(queryString)
responseDict = jsonResponse.json()

# Nice simple explanation of how to handle nested dictionary JSON responses:
# https://stackoverflow.com/questions/51788550/parsing-json-nested-dictionary-using-python
ticker = responseDict['ticker']
resultsDict = responseDict['results']
for x in resultsDict:
    openPx = x['o']
    highPx = x['h']
    lowPx = x['l']
    closePx = x['c']
    volume = x['v']
    trueRange = abs(highPx - lowPx)
    rawDate = x['t']
    convDate = dt.datetime.fromtimestamp(rawDate / 1000).strftime('%Y-%m-%d')

    cursor = dbConnect.cursor()
    query = ("IF OBJECT_ID('[pythondb].[us_historicaldata]') IS NOT NULL \
                        INSERT INTO us_historicaldata(Symbol, Date, Open, High, Low, Close, TR, Volume) \
                        VALUES({ticker}, {convDate}, {openPx}, {highPx}, {lowPx}, {closePx}, {truerange} \
                        , {volume} \
                        GO")
    cursor.execute(query)


    # print('Ticker: ' + str(ticker))
    # print('Open: ' + str(openPx))
    # print('High: ' + str(highPx))
    # print('Low: ' + str(lowPx))
    # print('Close: ' + str(closePx))
    # print('TrueRange: ' + str(trueRange))
    # print('Volume: ' + str(volume))
    # print('Date: ' + str(convDate))