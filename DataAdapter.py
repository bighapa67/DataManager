import os
import datetime as dt
import MySQLdb as sqldb
import pandas as pd
import atexit
import time
import traceback
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import logging
import TiingoData as tiingo
import PolygonData as poly
import EODData as eod

start = time.time()
recordCounter = 1
queryCounter = 1

# User defined parameters
startDate = '2019-11-06'
endDate = '2019-11-06'
dataFreq = 'daily'
unadjusted = 'false'

# CHOOSE YOUR OUTPUT DB TABLE:
########################################################################
table_name = 'pythondb.eodd_test'
# table_name = 'pythondb.tiingo_test'
# table_name = 'pythondb.polygon_test'
########################################################################

# Set the desired logging resolution here:
logging.basicConfig(filename='logging.log', level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(threadName)s - %(levelname)s - %(message)s')

os.environ['POLYGON_API_KEY'] = 'Xq_bQM92tq78l3FNagTWix06raWaq7y1ptr7_t'
os.environ['TIINGO_API_KEY'] = 'a3797816acfb2650321edf1ef4e06c3a69acde30'
os.environ['DB_USER'] = 'bighapa67'
os.environ['DB_PSWD'] = 'kando1'
os.environ['DB_HOST'] = 'localhost'
os.environ['DB_NAME'] = 'pythondb'
os.environ['EMAIL_ACCOUNT'] = 'jonestrading67@gmail.com'
os.environ['EMAIL_PSWD'] = 'Gkando!23O'


def GetElapsedTime():
    end = time.time()
    elapsedTime = end - start
    with open('C:\\Users\\Ken\\Documents\\Trading\\Spreadsheets\\ElapsedTime.txt', 'a+') as txtFile:
        # writer = csv.writer(txtFile, quotechar='"', quoting=csv.QUOTE_MINIMAL)
        # writer = csv.writer(txtFile, quoting=csv.QUOTE_MINIMAL)
        txtFile.write(f'ElapsedTime was: {elapsedTime} | Records: {recordCounter} | Records/Sec: '
                        f'{recordCounter/elapsedTime}\r')
    return elapsedTime


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

# Just enable the decorator if you want to receive a text when the program terminates.
# Nice to know if it shut down prematurely without having to constantly check on it.
@atexit.register
def FinishUp():
    print('Running closing routine...')
    sw = GetElapsedTime()
    SendSMS(sw)


# Array to hold our ticker symbols, regardless of the approach that is chosen.
tickers = []

#########################################
# Pandas approach

# Had to add the check to account for differing paths to Google Drive between laptop and desktop.
if os.path.exists('C:\\Users\\Ken\\Google Drive\\StockOdds\\StockOddsSymbols.csv'):
    symbols_df = pd.read_csv('C:\\Users\\Ken\\Google Drive\\StockOdds\\StockOddsSymbols.csv', index_col='Symbol',
                             skiprows=0)
elif os.path.exists('C:\\Users\\kjone\\Google Drive\\StockOdds\\StockOddsSymbols.csv'):
    symbols_df = pd.read_csv('C:\\Users\\kjone\\Google Drive\\StockOdds\\StockOddsSymbols.csv', index_col='Symbol',
                             skiprows=0)
else:
    print('User defined file path not found for symbols csv.')


for index, row in symbols_df.iterrows():
    ticker = index
    tickers.append(ticker)

##########################################

##########################################
# Short list approach

# tickers = ['ABR-A']
##########################################

dbConnect = sqldb.connect(user=os.environ['DB_USER'],
                          password=os.environ['DB_PSWD'],
                          host=os.environ['DB_HOST'],
                          database=os.environ['DB_NAME'],
                          use_unicode=True,
                          charset='utf8'
                          )

try:
    ###########################################################################
    # Send the entire ticker array to the data source helper
    # resultsDict = tiingo.GetData(startDate, endDate, dataFreq, tickers)
    # resultsDict = poly.GetData(startDate, endDate, tickers)
    resultsDict = eod.GetData(startDate, endDate, tickers)
    ###########################################################################

    for key, value in resultsDict.items():
        symbol = value.symbol[0]
        convDate = value.date[0]
        openPx = value.open[0]
        highPx = value.high[0]
        lowPx = value.low[0]
        closePx = value.close[0]
        trueRange = abs(highPx - lowPx)
        volume = value.volume

        cursor = dbConnect.cursor()

        # if symbol == 'FB':
        #     stop = 1

        try:
            query = f'INSERT INTO {table_name} (Symbol, Date, Open, High, Low, Close, TR, Volume)' \
                    f'VALUES("{symbol}", "{convDate}", {openPx}, {highPx}, {lowPx}, {closePx}, {trueRange}, {volume})'

            cursor.execute(query)
            recordCounter += 1
            queryCounter += 1
        except sqldb.IntegrityError:
            print(f'{symbol}: already exists in {table_name}')
            continue
        except:
            traceback.print_exc()
            print(f'Ticker: {symbol} failed to INSERT to the DB.')
            logging.info(f'Ticker: {symbol} failed to INSERT to the DB.')
            continue
        finally:
            cursor.close()
except:
    traceback.print_exc()
    print('Something went wrong with the query results')
    print(f'Error on ticker: {ticker}')
    logging.info(f'Ticker: {ticker} failed while attempting to parse the JSON response (responseDict)')
    cursor.close()
    # continue
finally:
    print(f'Writing data to {table_name}')
    dbConnect.commit()