import pandas as pd
import os
import urllib
# import SqlConnection as sc
from database_connector import DatabaseConnector
from sqlalchemy import Table, select, join
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime, timedelta

"""
This program is meant to be the conductor that orchestrates the capture and recording of my CEF related data.
The main components are the price and NAV of my closed-end funds.
Dividends are also of vital concern, but for now I'm going to separate that process out.
"""

# Load environment variables from the .env file
load_dotenv()

# Create instance(s) of the DatabaseConnector class for the desired database(s).
# Engines are automatically created in the DatabaseConnector's class constructor.
# Note: Ensure that your .env file or environment has all the required variables.
HistoricalPriceDB_conn = DatabaseConnector("HistoricalPriceDB")
RealTick_LiveData_conn = DatabaseConnector("RealTick_LiveData")

# symbol_source_df  = pd.DataFrame()

def load_symbols():
    """
    This function is meant to load the symbols of the CEFs that I want to track.
    :return: a list of symbols
    """

    # column_names = ['Symbol', 'Date', 'MFQSSym', 'ClosePx', 'NAV']
    column_names = ['Symbol']
    # filters = [('Date', 'gt', '2024-04-18')]
    results = HistoricalPriceDB_conn.read_from_mssql("CEF_Setup", column_names)

    symbol_source_df = pd.DataFrame(results)
    # print(symbol_source_df)
    return symbol_source_df


def retrieve_joined_data(data_filter=None):
    # Define table names and columns
    symbol_table = "CEF_Setup"
    symbol_table_columns = ["Symbol", "MFQSSym"]
    price_table = "RealTimeData"
    price_table_columns = ["Symbol", "TimeUpdated", "Open", "DayHigh", "DayLow", "CurrentPrice"]
    nav_table = "RealtickNAVs"
    # "Symbol" in the RealtickNAVs table is actually the "MFQSSym" and "ClosePx" = "NAV".
    nav_table_columns = ["Symbol", "Date", "ClosePx"]

    # Build the Table objects
    symbol_table_obj = Table(symbol_table, HistoricalPriceDB_conn.metadata, autoload_with=HistoricalPriceDB_conn.engine)
    price_table_obj = Table(price_table, RealTick_LiveData_conn.metadata, autoload_with=RealTick_LiveData_conn.engine)
    nav_table_obj = Table(nav_table, HistoricalPriceDB_conn.metadata, autoload_with=HistoricalPriceDB_conn.engine)
    
    # Build the JOIN statements
    # symbol_table and price_table will join on "Symbol".
    join_condition = symbol_table_obj.c.Symbol == price_table_obj.c.Symbol
    columns_to_select = [getattr(symbol_table_obj.c, col) for col in symbol_table_columns] + \
                        [getattr(price_table_obj.c, col) for col in price_table_columns] + \
                        [getattr(nav_table_obj.c, col) for col in nav_table_columns]

    # stmt = select(*columns_to_select).select_from(
    #     join(symbol_table_obj, price_table_obj, join_condition)
    #     ).where(price_table_obj.c.Symbol == symbol_table_obj.c.Symbol)  # Applying date filter to the join

    stmt = select(*columns_to_select).select_from(
        symbol_table_obj.join(price_table_obj, symbol_table_obj.c.Symbol == price_table_obj.c.Symbol).join(
            nav_table_obj, symbol_table_obj.c.MFQSSym == nav_table_obj.c.Symbol))
      
    # symbol_table and nav_table will join on symbol_table's "MFQSSym" and nav_table's "Symbol".
    
    # Execute the query
    Session = sessionmaker(bind=RealTick_LiveData_conn.engine)
    with Session() as session:
        results = session.execute(stmt).fetchall()
        return results

def fetch_data(db_connector, table_name, columns):
    """ Fetches data from a specific table and returns it as a DataFrame. """

    Session = sessionmaker(bind=db_connector)

    with Session() as session:
        query = f"SELECT {', '.join(columns)} FROM {table_name}"
        df = pd.read_sql(query, db_connector.engine)
    return df

def merge_dataframes(symbol_df, price_df, nav_df):
    """ Merges DataFrames on specified keys. """
    # merged_df = symbol_df.merge(price_df, on="Symbol").merge(df3, on=join_keys)
    merged_df = pd.merge(symbol_df, price_df, on="Symbol").merge(nav_df, left_on="MFQSSym", right_on="Symbol")
    return merged_df

def process_columns(df):
    """ Renames columns in a DataFrame. """
    df.rename(columns={"Symbol_x": "Symbol", "Open": "DayOpen", "CurrentPrice":"DayClose", "ClosePx":"NAV"}, inplace=True)
    df.drop(columns=["Symbol_y"], inplace=True)
    df['TimeUpdated'] = pd.to_datetime(df['TimeUpdated'])
    # df['TimeUpdated'] = pd.to_datetime(df['TimeUpdated'], format='%Y-%m-%d')
    df['TimeUpdated'] = df['TimeUpdated'].dt.date
    return df

def date_check(df):
    """
    Check if the price date and NAV date are the same. 
    I think I'm going to pick three funds and compare THOSE dates.
    If the dates don't match then I need to figure out how to handle that.
    1) I could use a filter to create a df of only the matching dates that also match the expected date.
       Those records would then go through bulk insertion with the offenders being logged and reported.
    2) Alert me of the issue and I'll manually handle it.
    """
  
    df['date_matches'] = df['TimeUpdated'] == df['Date']
    print(df['date_matches'].value_counts())
    print(df[df['date_matches'] == False])

    return df

def determine_session():
    """
    This function will determine the session to use based on the time of day.
    It will then use that information to determine the date to use for validation.
    The RealTick NAV data grab is sometimes done at 7pm CT (session "T") and sometimes the next morning
    (session "T+1"), resulting in discrepancies in the dates in the tables when compared to "today".
    """
    today = datetime.today()
    if today.hour < 19:
        print("Running the ""Date T-1"" session (i.e. grabbing yesterday's final NAV data).")
        validation_date = today.date() - timedelta(days=1)
        print(validation_date)
    else:
        print("Running the ""Date T"" session. We are expecting the NAV data to have been updated for today.")
        validation_date = today.date()

    return validation_date


if __name__ == "__main__":
    validation_date = determine_session()
    symbol_df = fetch_data(HistoricalPriceDB_conn, "CEF_Setup", ["Symbol", "MFQSSym"])
    price_df = fetch_data(RealTick_LiveData_conn, "RealTimeData", ["Symbol", "TimeUpdated", "[Open]", "DayHigh", "DayLow", "CurrentPrice"])
    nav_df = fetch_data(HistoricalPriceDB_conn, "RealtickNAVs", ["Symbol", "Date", "ClosePx"])
    merged_df = merge_dataframes(symbol_df, price_df, nav_df)
    processed_df = process_columns(merged_df)
    # This is a good time to do a check on the price date (coming from RealTimeData) and the NAV date (coming from RealtickNAVs).
    date_check_df = date_check(processed_df)

    # Make two separate dfs for the records that match and those that don't.
    # The matching records will be bulk inserted into the CEF_price_nav_history table.
    # I do need to run a check on the history table and/or have an alert sent if there's an issue.
    # I need to account for the variability of when this script runs.

    print(processed_df.dtypes)
    print()
    print(processed_df)
    breakpoint = 0