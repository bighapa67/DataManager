import requests
import os
import datetime as dt
import MySQLdb as sqldb
import csv
import pandas as pd
import atexit
import time

start = time.time()
recordCounter = 1

def GetElapsedTime():
    end = time.time()
    elapsedTime = end - start
    with open('C:\\Users\\Ken\\Documents\\Trading\\Spreadsheets\\ElapsedTime.txt', 'a+') as txtFile:
        # writer = csv.writer(txtFile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # writer = csv.writer(txtFile, quoting=csv.QUOTE_MINIMAL)
        txtFile.write(f'ElapsedTime was: {elapsedTime} | Records: {recordCounter} | Records/Sec: '
                        f'{recordCounter/elapsedTime}\r')


os.environ['API_KEY'] = 'Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t'
os.environ['DB_USER'] = 'bighapa67'
os.environ['DB_PSWD'] = 'kando1'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'pythondb'

startDate = '2019-08-01'
endDate = '2019-10-25'
unadjusted = 'false'
histRangeUrl = 'https://api.polygon.io/v2/aggs/ticker/'

# Need to clean this up.
# Read tickers from a csv file
# with open('C:\\Users\\Ken\\Documents\\Trading\\Spreadsheets\\MasterReferenceSymbolList.csv', 'rt') as csvFile:
tickers = []

#########################################
# Pandas approach

# symbols_df = pd.read_csv('C:\\Users\\Ken\\Documents\\Trading\\Spreadsheets\\'
#                          'StockOddsSymbols.csv', index_col='Symbol', skiprows=0)
symbols_df = pd.read_csv('C:\\Users\\Ken\\Downloads\\StockOddsSymbols.csv', index_col='Symbol', skiprows=0)
#symbols_df = pd.read_csv('C:\\Users\\Ken\\Downloads\\StockOddsSymbols.csv', skiprows=0)

# for index, row in symbols_df.iterrows():
#     ticker = index
#     tickers.append(ticker)
##########################################

##########################################
# Short list approach

tickers = ['ABR-A']
##########################################

# Still need to parameterize the MySql connection.
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

for ticker in tickers:
    queryString = (histRangeUrl + ticker + '/range/1/day/' + startDate + '/' + endDate + '?unadjusted='
                   + unadjusted + '&apiKey=' + os.environ['API_KEY'])

    jsonResponse = requests.get(queryString)
    responseDict = jsonResponse.json()

    # Nice simple explanation of how to handle nested dictionary JSON responses:
    # https://stackoverflow.com/questions/51788550/parsing-json-nested-dictionary-using-python
    ticker = str(responseDict['ticker']).upper()
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

        # query = "BEGIN \
        #         IF OBJECT_ID('[pythondb].[us_historicaldata]') IS NOT NULL \
        #             INSERT INTO us_historicaldata(Symbol, Date, Open, High, Low, Close, TR, Volume) \
        #             VALUES({ticker}, {convDate}, {openPx}, {highPx}, {lowPx}, {closePx}, {truerange} \
        #             , {volume} \
        #             GO"

        # query = f'INSERT INTO pythondb.us_historicaldata (Symbol, Date, Open, High, Low, Close, TR, Volume)' \
        #         f'VALUES("{ticker}", "{convDate}", {openPx}, {highPx}, {lowPx}, {closePx}, {trueRange}, {volume})'

        try:
            query = f'INSERT INTO pythondb.us_historicaldata (Symbol, Date, Open, High, Low, Close, TR, Volume)' \
                    f'VALUES("{ticker}", "{convDate}", {openPx}, {highPx}, {lowPx}, {closePx}, {trueRange}, {volume})'

            cursor.execute(query)
            dbConnect.commit()
            recordCounter += 1
        except sqldb._exceptions.IntegrityError:
            continue
        finally:
            cursor.close()

        # print('Ticker: ' + str(ticker))
        # print('Open: ' + str(openPx))
        # print('High: ' + str(highPx))
        # print('Low: ' + str(lowPx))
        # print('Close: ' + str(closePx))
        # print('TrueRange: ' + str(trueRange))
        # print('Volume: ' + str(volume))
        # print('Date: ' + str(convDate))
atexit.register(GetElapsedTime)