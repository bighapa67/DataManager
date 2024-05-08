import sys
import pandas as pd
import os
import urllib
# import SqlConnection as sc
from database_connector import DatabaseConnector
from table_schemas.CEF_price_nav_history import CEF_price_nav_history
from sqlalchemy import Table, select, join
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv
from datetime import datetime, timedelta
from dataclasses import fields

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

    # Build the SELECT statement
    stmt = select(*columns_to_select).select_from(
        symbol_table_obj.join(price_table_obj, symbol_table_obj.c.Symbol == price_table_obj.c.Symbol).join(
            nav_table_obj, symbol_table_obj.c.MFQSSym == nav_table_obj.c.Symbol))
      
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

    merged_df = pd.merge(symbol_df, price_df, on="Symbol").merge(nav_df, left_on="MFQSSym", right_on="Symbol")
    return merged_df


def process_columns_cef_price_nav_history(df):
    """ 
    Renames columns in a DataFrame.
    The column names have been chosen to reflect the columns in the dbo.CEF_price_nav_history table.
    """

    df.rename(columns={"Symbol_x": "Symbol", "Open": "OpenPx", "DayHigh": "HighPx", "DayLow": "LowPx", "ClosePx":"NAV", "CurrentPrice":"ClosePx"}, inplace=True)
    df.drop(columns=["Symbol_y"], inplace=True)
    # print(df)

    return df


def final_date_check(df):
    """
    Final check to ensure the price date and NAV date are the same. 
    I think I'm going to pick three funds and compare THOSE dates.
    If the dates don't match then I need to figure out how to handle that.
    1) I could use a filter to create a df of only the matching dates that also match the expected date.
       Those records would then go through bulk insertion with the offenders being logged and reported.
    2) Alert me of the issue and I'll manually handle it.
    """
  
    df['date_matches'] = df['TimeUpdated'] == df['Date']
    # df['date_incorrect'] = df['TimeUpdated'] != df['Date']
    print(df['date_matches'].value_counts())
    print()
    print("Records where the Price and NAV dates do not match:\n")
    print(df[df['date_matches'] == False])
    print()

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
        # NOTE: This must be run prior to turning LDUS on on the morning of "Date T" or else 
        # "CurrentPrice" in dbo.RealTimeData will be incorrect.
        print("Running the ""Date T-1"" session (i.e. grabbing yesterday's final NAV data).")
        validation_date = today.date() - timedelta(days=1)
        print(f'validation_date: {validation_date}\n')
    else:
        print("Running the ""Date T"" session. We are expecting the NAV data to have been updated for today.")
        validation_date = today.date()
        print(f'validation_date: {validation_date}\n')

    return validation_date


def price_nav_validation(validation_date, price_validation_symbols, nav_validation_symbols, price_df, nav_df):
    
    validation_test = 0
    validation_result = None
    
    # Filter DataFrames for specified symbols
    price_check = price_df[price_df['Symbol'].isin(price_validation_symbols)]
    nav_check = nav_df[nav_df['Symbol'].isin(nav_validation_symbols)]

    # Compare dates
    price_check['PriceDateMatch'] = price_check['TimeUpdated'] == validation_date
    nav_check['NavDateMatch'] = nav_check['Date'] == validation_date

    if price_check['PriceDateMatch'].sum() == len(price_check['PriceDateMatch']):
        print("Price date validation = PASS")
        validation_test += 1
    else:
        print("Price date validation = FAIL")
        validation_test -= 1

    if nav_check['NavDateMatch'].sum() == len(nav_check['NavDateMatch']):
        print("NAV date validation = PASS")
        validation_test += 1
    else:
        print("NAV date validation = FAIL")
        validation_test -= 1

    if validation_test == 2:
        print("Validation test = PASS")
        validation_result = True
    else:
        print("Validation test = FAIL")
        validation_result = False
    
    return validation_result


def create_df_instances(row):
    return CEF_price_nav_history(**row)


if __name__ == "__main__":
    # Create an instance of the DatabaseConnector class
    # Note: Ensure that your .env file or environment has all the required variables.
    HistoricalPriceDB_conn = DatabaseConnector("HistoricalPriceDB")

    # Determine which "session" we're in based on the time of day.
    validation_date = determine_session()

    # Fetch the desired tables and columns from the databases.
    symbol_df = fetch_data(HistoricalPriceDB_conn, "CEF_Setup", ["Symbol", "MFQSSym"])
    price_df = fetch_data(RealTick_LiveData_conn, "RealTimeData", ["Symbol", "TimeUpdated", "[Open]", "DayHigh", "DayLow", "CurrentPrice"])
    nav_df = fetch_data(HistoricalPriceDB_conn, "RealtickNAVs", ["Symbol", "Date", "ClosePx"])

    # The "TimeUpdated" column in dbo.RealTimeData has nanosecond resolution that we need to strip.
    # Before performing the final step of specifying formatting ALL three date fields, boolean comparisons
    # would always return False.
    price_df['TimeUpdated'] = pd.to_datetime(price_df['TimeUpdated'], errors='coerce', format='%Y-%m-%d')
    price_df['TimeUpdated'] = price_df['TimeUpdated'].dt.normalize()

    nav_df['Date'] = pd.to_datetime(nav_df['Date'], format='%Y-%m-%d')
    nav_df['Date'] = nav_df['Date'].dt.normalize()
    
    validation_date = pd.to_datetime(validation_date, format='%Y-%m-%d')
    
    # Before we get into proccessing the data, let's run a quick verification on our price
    # and NAV dates for select liquid funds.
    price_validation_symbols = ["STEW", "QQQX"]
    nav_validation_symbols = ["XSTEX", "XQQQX"]
    validation_result = price_nav_validation(validation_date, price_validation_symbols, nav_validation_symbols, price_df, nav_df)
    # price_df, nav_df = price_nav_validation(validation_date, price_validation_symbols, nav_validation_symbols, price_df, nav_df)
    
    if validation_result:
        print("Validation successful. Proceeding with data processing.\n")
        merged_df = merge_dataframes(symbol_df, price_df, nav_df)

        # Process the columns of the merged df to match the CEF_price_nav_history table schema.
        processed_df = process_columns_cef_price_nav_history(merged_df)
        # print(f"processed_df:\n{processed_df}\n")
        
        # This is a good time to do a final check on the price date (coming from RealTimeData) vs
        # the NAV date (coming from RealtickNAVs) before running our upsert.
        # TODO: I need to figure out how to handle the records where the dates don't match.
        date_checked_df = final_date_check(processed_df)
        print(f"date_checked_df:\n{date_checked_df}\n")

        # For now I'm going to filter date_checked_df to only include rows where the price and NAV dates match.
        date_checked_df = date_checked_df[date_checked_df['date_matches'] == True]
        print(f"date_checked_df:\n{date_checked_df}\n")

        # Incredible GPT4 technique leveraging dataclasses.
        # We use list comprehension to grab the names of the fields we require to satisfy our table schema.
        # Then we filter the dataframe to only include those columns!!!
        required_columns = [field.name for field in fields(CEF_price_nav_history)]
        upsert_ready_df = date_checked_df[required_columns]
        print(f"upsert_ready_df:\n{upsert_ready_df}\n")

        # Convert each row of the df into an instance of our data class for SQL table insertion.
        data_class_instances = upsert_ready_df.apply(create_df_instances, axis=1)

        # TODO: Make two separate dfs for the records that match and those that don't.
        # TODO: The program runs so fast that it's not possible to read the console output.
        #   Need to create a log file that captures summary data for the run.
        # The matching records will be bulk inserted into the CEF_price_nav_history table.
        # I do need to run a check on the history table and/or have an alert sent if there's an issue.
        # I need to account for the variability of when this script runs.

        # Upsert the new data to the table
        HistoricalPriceDB_conn.upsert_price_record_instances_mssql("CEF_price_nav_history", data_class_instances)
    else:
        print("Final date validation failed. Data processing will not proceed.")
        sys.exit()
