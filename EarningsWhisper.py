import pandas as pd
import numpy as np
import datetime as dt
import pyodbc
import requests
from bs4 import BeautifulSoup
from database_connector import DatabaseConnector
from sqlalchemy import create_engine
import re
import datetime as dt
# import json


def get_data(url):
    # url = 'https://www.earningswhispers.com/calendar'
    response = requests.get(url)
    soup = BeautifulSoup(response.content, "html.parser")

    # Click "Show More" button to reveal all data
    show_more_button = soup.find('button', {'id': 'showMoreButton'})
    if show_more_button:
        show_more_url = 'https://www.earningswhispers.com' + show_more_button['onclick'].split("'")[1]
        show_more_response = requests.get(show_more_url)
        soup = BeautifulSoup(show_more_response.content, 'html.parser')

    # Find all the <li> tags with the specified class
    li_tags = soup.find_all("li", class_=['cor', 'cors'])

    # Extract the date from the <script> tag
    json_ld_string = soup.find("script", type="application/ld+json")
    json_ld_string = str(json_ld_string)

    # Use a regular expression to extract the startDate (json didn't work
    start_date_str = re.search('"startDate" : "(.*?)"', json_ld_string).group(1)

    # Parse the startDate string into a datetime object
    start_date = dt.datetime.strptime(start_date_str, '%Y-%m-%dT%H:%M%SZ')

    # Extract the data from each tag using a list comprehension
    data = [
        {
            "Ticker": li_tag.find("div", class_="ticker").text.strip(),
            "AnnTime": li_tag.find("div", class_="time").text.strip(),
            "EPS_est": li_tag.find("div", class_="estimate").text.strip(),
            "Rev_est": li_tag.find("div", class_="revest").text.strip(),
            "RevGrowth_est": li_tag.find("div", class_="revgrowthprint").text.strip()
        }
        for li_tag in li_tags
    ]

    # Create a Pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(data)

    # Extract the date from the datetime object and add it to the DataFrame
    df['Date'] = start_date.date()

    return(df)


def connect_to_db(server, database, driver):
    connector = DatabaseConnector(server, database)
    connection = connector.get_connection()
    cursor = connection.cursor()
    # engine = create_engine('mssql+pyodbc://FRANKENSTEIN\SQLEXPRESS/TimeSeriesDB?driver=ODBC+Driver+17+for+SQL+Server')
    engine = create_engine(f'mssql+pyodbc://{server}/{database}?driver={driver}')

    return connector, connection, cursor, engine


def transform_datatypes(data_df):
    # First we need to clean any strange characters from the data
    data_df['EPS_est'] = data_df['EPS_est'].str.replace('$', '')
    data_df['EPS_est'] = data_df['EPS_est'].str.replace('(', '-')
    data_df['EPS_est'] = data_df['EPS_est'].str.replace(')', '')

    data_df['Rev_est'] = data_df['Rev_est'].str.replace('$', '')
    data_df['Rev_est'] = data_df['Rev_est'].str.replace(' M', '')
    data_df['Rev_est'] = data_df['Rev_est'].str.replace(' B', '')
    data_df['RevGrowth_est'] = data_df['RevGrowth_est'].str.replace('%', '')

    # convert the values to numbers and set non-numeric values to NaN
    # EarningsWhispers uses a dash to indicate no data, so we need to convert that to NaN.
    # data_df['EPS_est'] = pd.to_numeric(data_df['EPS_est'], errors='coerce')
    # data_df['Rev_est'] = pd.to_numeric(data_df['Rev_est'], errors='coerce')
    # data_df['RevGrowth_est'] = pd.to_numeric(data_df['RevGrowth_est'], errors='coerce')
    data_df = data_df.replace('-', np.nan)

    # Format our numerical data to the correct data types
    data_df['EPS_est'] = data_df['EPS_est'].astype(float)
    data_df['Rev_est'] = data_df['Rev_est'].astype(float)
    data_df['RevGrowth_est'] = data_df['RevGrowth_est'].astype(float)
    data_df['RevGrowth_est'] = data_df['RevGrowth_est']/100

    return data_df


if __name__ == '__main__':
    # Connect to the database
    server = 'FRANKENSTEIN\SQLEXPRESS'
    database = 'TimeSeriesDB'
    driver = 'ODBC+Driver+17+for+SQL+Server'
    table = 'EarningsWhispers'

    # The second url shows the format for getting data for future days.
    # "2&T" is T+1 (tomorrow), "3&T" is T+2 (day after tomorrow), etc.
    url = 'https://www.earningswhispers.com/calendar'
    # url = 'https://www.earningswhispers.com/calendar?sb=c&d=2&t=all&v=s'

    connector, connection, cursor, engine = connect_to_db(server, database, driver)

    data_df = get_data(url)
    data_df = transform_datatypes(data_df)

    # Reorder the columns so they make sense visually in case we need to inspect the data
    columns = ['Date', 'Ticker', 'AnnTime', 'EPS_est', 'Rev_est', 'RevGrowth_est']
    data_df = data_df.reindex(columns=columns)

    # check if the data already exists in the table
    # exists = pd.read_sql_query(f'SELECT 1 FROM {table} WHERE "Date" = data_df["Date"] AND "Ticker" = data_df["Ticker"]',
    #                            engine)

    # insert the data into the table if it doesn't exist
    # if not exists.empty:
    #     # Write the data to the database
    #     data_df.to_sql(table, engine, if_exists='append', index=False)
    # else:
    #     # update the existing record
    #     pass

    # Write the data to the database
    # It may be necessary in the future to evaluate the df line-by-line if somehow some records for the day
    # already exist, but others don't.  I can't readily think of how that would happen though.
    try:
        data_df.to_sql(table, engine, if_exists='append', index=False)
        print('Records added successfully!!!')
    except Exception as e:
        error_str = e.args[0]
        if 'pyodbc.IntegrityError' in error_str:
            print('Record already exists')
            print(error_str)
        else:
            print(e)

    # print(data_df.info())
    # print(data_df)

    # Close the connection
    connector.close_connection(connection)