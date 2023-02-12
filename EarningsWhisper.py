import pandas as pd
import datetime as dt
import requests
from bs4 import BeautifulSoup
from database_connector import DatabaseConnector
from sqlalchemy import create_engine
import re
import datetime as dt
# import json


# def get_data():
#     url = 'https://www.earningswhispers.com/calendar'
#     response = requests.get(url)
#
#     soup = BeautifulSoup(response.content, 'html.parser')
#
#     # Click "Show More" button to reveal all data
#     show_more_button = soup.find('button', {'id': 'showMoreButton'})
#     if show_more_button:
#         show_more_url = 'https://www.earningswhispers.com' + show_more_button['onclick'].split("'")[1]
#         show_more_response = requests.get(show_more_url)
#         soup = BeautifulSoup(show_more_response.content, 'html.parser')
#
#     # Extract the data from the table
#     table = soup.find('table', {'id': 'ecdata'})
#     df = pd.read_html(str(table))[0]
#
#     # Filter for only the data for Monday, 2/13
#     df = df[df['Date'] == '02/13/2023']
#
#     return(df)


def get_data2():
    url = 'https://www.earningswhispers.com/calendar'
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


def connect_to_db(server, database):

    table = 'EarningsWhispers'
    connector = DatabaseConnector('FRANKENSTEIN\SQLEXPRESS', 'TimeSeriesDB')
    connection = connector.get_connection()
    cursor = connection.cursor()
    engine = create_engine('mssql+pyodbc://FRANKENSTEIN\SQLEXPRESS/TimeSeriesDB?driver=ODBC+Driver+17+for+SQL+Server')
    # engine = create_engine(
    #     "mssql+pyodbc://your_username:your_password@your_server/your_database?driver=ODBC+Driver+17+for+SQL+Server")

    # Test the connection
    # query = "SELECT * FROM dbo.DummyTable4"
    # cursor.execute(query)
    # rows = cursor.fetchall()
    # columns = [column[0] for column in cursor.description]
    # df = pd.DataFrame.from_records(data=rows, columns=columns)
    return connector, connection, cursor, engine


if __name__ == '__main__':
    # Connect to the database
    server = 'FRANKENSTEIN\SQLEXPRESS'
    database = 'TimeSeriesDB'
    connector, connection, cursor, engine = connect_to_db(server, database)

    data_df = get_data2()
    columns = ['Date', 'Ticker', 'AnnTime', 'EPS_est', 'Rev_est', 'RevGrowth_est']
    data_df = data_df.reindex(columns=columns)

    # Write the data to the database
    data_df.to_sql('EarningsWhispers', engine, if_exists='append', index=False)

    print(data_df.info())
    print(data_df)

    # Close the connection
    connector.close_connection(connection)