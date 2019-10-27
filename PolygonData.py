import requests
import os
import datetime as dt
import MySQLdb as sqldb
import csv
import pandas as pd
import atexit
import time
from tqdm import tqdm
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

start = time.time()
recordCounter = 1
queryCounter = 1

os.environ['API_KEY'] = 'Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t'
os.environ['DB_USER'] = 'bighapa67'
os.environ['DB_PSWD'] = 'kando1'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'pythondb'
os.environ['EMAIL_ACCOUNT'] = 'jonestrading67@gmail.com'
os.environ['EMAIL_PSWD'] = 'Gkando!23O'


# @atexit.register
def GetElapsedTime():
    end = time.time()
    elapsedTime = end - start
    with open('C:\\Users\\Ken\\Documents\\Trading\\Spreadsheets\\ElapsedTime.txt', 'a+') as txtFile:
        # writer = csv.writer(txtFile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # writer = csv.writer(txtFile, quoting=csv.QUOTE_MINIMAL)
        txtFile.write(f'ElapsedTime was: {elapsedTime} | Records: {recordCounter} | Records/Sec: '
                        f'{recordCounter/elapsedTime}\r')
    return elapsedTime


# @atexit.register
def SendSMS(sw):
    email = os.environ['EMAIL_ACCOUNT']
    pas = os.environ['EMAIL_PSWD']

    sms_gateway = '7735512347@txt.att.net'
    smtp = "smtp.gmail.com"
    port = 587
    # This will start our email server
    server = smtplib.SMTP(smtp, port)
    # Starting the server
    server.starttls()
    # Now we need to login
    server.login(email, pas)

    now = dt.datetime.now()
    current_time = now.strftime('%H:%M:%S')

    # Now we use the MIME module to structure our message.
    msg = MIMEMultipart()
    msg['From'] = email
    msg['To'] = sms_gateway

    # Make sure you add a new line in the subject
    msg['Subject'] = "Data get alert.\n"

    # Make sure you also add new lines to your body
    body = f"Data download completed @ {current_time}; in {round(sw/60,2)} minutes.\n"

    # and then attach that body furthermore you can also send html content.
    msg.attach(MIMEText(body, 'plain'))

    sms = msg.as_string()
    server.sendmail(email, sms_gateway, sms)

    # lastly quit the server
    server.quit()


@atexit.register
def FinishUp():
    print('Running closing routine...')
    sw = GetElapsedTime()
    SendSMS(sw)


startDate = '2019-01-01'
endDate = '2019-12-31'
unadjusted = 'false'
histRangeUrl = 'https://api.polygon.io/v2/aggs/ticker/'
# https://api.polygon.io/v2/aggs/ticker/AAPL/range/1/day/2019-10-20/2019-12-31?apiKey=Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t

# array to hold our ticker symbols
tickers = []

#########################################
# Pandas approach

symbols_df = pd.read_csv('C:\\Users\\Ken\\Downloads\\StockOddsSymbols.csv', index_col='Symbol', skiprows=0)

# Currently skipping symbols with '-' in them as I can't figure out the format Polygon wants.
for index, row in symbols_df.iterrows():
    ticker = index
    if '-' in ticker:
        continue
    else:
        tickers.append(ticker)
##########################################

##########################################
# Short list approach

# tickers = ['ABR-A']
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

# I wonder how to accomplish the 'iterable check' without having to copy the entire code block
# over again except for the 'tqdm(tickers)' bit.

pbar = tqdm(total=len(tickers))

for ticker in tickers:
    pbar.update(1)

    try:
        queryString = (histRangeUrl + ticker + '/range/1/day/' + startDate + '/' + endDate + '?unadjusted='
                       + unadjusted + '&apiKey=' + os.environ['API_KEY'])
    except:
        print(f'Fuck, something went wrong with ticker {ticker}')
        traceback.print_exc()

    jsonResponse = requests.get(queryString)
    responseDict = jsonResponse.json()

    # Nice simple explanation of how to handle nested dictionary JSON responses:
    # https://stackoverflow.com/questions/51788550/parsing-json-nested-dictionary-using-python
    ticker = str(responseDict['ticker']).upper()
    resultsDict = responseDict['results']
    try:
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

            try:
                # This pause was necessary as Polygon seemed to block me at around 1000 requests in some
                # short amount of time.
                # if queryCounter % 1000 == 0:
                #     time.sleep(1)  # in seconds
                #     queryCounter = 1

                query = f'INSERT INTO pythondb.us_historicaldata (Symbol, Date, Open, High, Low, Close, TR, Volume)' \
                        f'VALUES("{ticker}", "{convDate}", {openPx}, {highPx}, {lowPx}, {closePx}, {trueRange}, {volume})'

                if queryCounter % 1000 == 0:
                    time.sleep(1)  # in seconds
                    queryCounter = 1
                    dbConnect.commit()

                cursor.execute(query)
                # dbConnect.commit()
                recordCounter += 1
                queryCounter += 1
            except sqldb._exceptions.IntegrityError:
                continue
            finally:
                cursor.close()
    except:
        print('Something went wrong with the JSON results')
        print(f'Error on ticker: {ticker}')
        traceback.print_exc()
        continue



            # print('Ticker: ' + str(ticker))
            # print('Open: ' + str(openPx))
            # print('High: ' + str(highPx))
            # print('Low: ' + str(lowPx))
            # print('Close: ' + str(closePx))
            # print('TrueRange: ' + str(trueRange))
            # print('Volume: ' + str(volume))
            # print('Date: ' + str(convDate))
# else:
#     print('No iterable list was presented... \r\n'
#           'Make sure the .csv file is not open and contains more than one symbol.')

#atexit.register(FinishUp)