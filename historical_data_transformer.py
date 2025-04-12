# For detailed usage instructions and examples, see ./docs/historical_data_transformer_cli.md
import pandas as pd
import argparse
from datetime import datetime
from database_connector_v1 import DatabaseConnector
import sys
import os

# Add the project root to the Python path to allow importing database_connector_v1
# Assuming the script is run from the same directory or a standard location relative to the root
# Adjust the path depth (../) if necessary based on where this script is located
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

try:
    from database_connector_v1 import DatabaseConnector
except ImportError:
    print("Error: Could not import DatabaseConnector. Ensure database_connector_v1.py is accessible.")
    sys.exit(1)

def fetch_historical_data(db_connector, table_name, target_date):
    """
    Fetches historical data from the specified table for a given date.

    Args:
        db_connector: An instance of the DatabaseConnector.
        table_name (str): The name of the table to fetch from (e.g., 'TestHistoricalData').
        target_date (str): The date to filter data for (YYYY-MM-DD format).

    Returns:
        pandas.DataFrame: A DataFrame containing the data for the target date,
                          or an empty DataFrame if no data is found or an error occurs.
    """
    try:
        # Validate date format
        datetime.strptime(target_date, '%Y-%m-%d')
    except ValueError:
        print(f"Error: Invalid date format '{target_date}'. Please use YYYY-MM-DD.")
        return pd.DataFrame()

    # Define columns based on the TestHistoricalData schema provided
    columns_to_fetch = ["Symbol", "Date", "OpenPx", "HighPx", "LowPx", "ClosePx"]
    filters = [("Date", "eq", target_date)]

    try:
        results = db_connector.read_from_mssql(table_name, columns_to_fetch, filters)
        if results:
            df = pd.DataFrame(results)
            print(f"Successfully fetched {len(df)} records from {table_name} for date {target_date}.")
            return df
        else:
            print(f"No data found in {table_name} for date {target_date}.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching data from {table_name} for date {target_date}: {e}")
        return pd.DataFrame()


def fetch_symbol_mapping(db_connector):
    """
    Fetches the symbol mapping from the CEF_Setup table.

    Args:
        db_connector: An instance of the DatabaseConnector for HistoricalPriceDB.

    Returns:
        pandas.DataFrame: A DataFrame containing 'Symbol' and 'MFQSSym' columns,
                          or an empty DataFrame if an error occurs.
    """
    table_name = "CEF_Setup"
    columns_to_fetch = ["Symbol", "MFQSSym"]
    print(f"Fetching symbol mapping from {table_name}...")
    try:
        results = db_connector.read_from_mssql(table_name, columns_to_fetch)
        if results:
            df = pd.DataFrame(results)
            print(f"Successfully fetched {len(df)} symbol mappings.")
            return df
        else:
            print(f"No symbol mappings found in {table_name}.")
            return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching symbol mapping from {table_name}: {e}")
        return pd.DataFrame()


def transform_price_data(df):
    """
    Transforms the raw historical data into the price_df format expected by DataConductor.

    Args:
        df (pandas.DataFrame): DataFrame containing raw price data.

    Returns:
        pandas.DataFrame: Transformed DataFrame matching the price_df structure.
    """
    if df.empty:
        return df

    # Rename columns to match the structure expected after fetching from RealTimeData
    df_renamed = df.rename(columns={
        "Date": "TimeUpdated",
        "OpenPx": "Open",
        "HighPx": "DayHigh",
        "LowPx": "DayLow",
        "ClosePx": "CurrentPrice"
    })

    # Select only the required columns
    required_columns = ["Symbol", "TimeUpdated", "Open", "DayHigh", "DayLow", "CurrentPrice"]
    # Ensure all required columns exist, add missing ones with None/NaN if necessary
    for col in required_columns:
        if col not in df_renamed.columns:
            df_renamed[col] = None # Or pd.NA

    df_transformed = df_renamed[required_columns]

    # Ensure date format consistency (though fetch should handle this)
    df_transformed['TimeUpdated'] = pd.to_datetime(df_transformed['TimeUpdated']).dt.normalize()

    return df_transformed

def transform_nav_data(df):
    """
    Transforms the raw historical data into the nav_df format expected by DataConductor.

    Args:
        df (pandas.DataFrame): DataFrame containing raw NAV data.

    Returns:
        pandas.DataFrame: Transformed DataFrame matching the nav_df structure.
    """
    if df.empty:
        return df

    # Select required columns. Renaming isn't strictly necessary as 'ClosePx' matches,
    # but we explicitly select to be clear.
    required_columns = ["Symbol", "Date", "ClosePx"]
    # Ensure all required columns exist, add missing ones with None/NaN if necessary
    for col in required_columns:
        if col not in df.columns:
            df[col] = None # Or pd.NA

    df_transformed = df[required_columns]

    # Ensure date format consistency (though fetch should handle this)
    df_transformed['Date'] = pd.to_datetime(df_transformed['Date']).dt.normalize()

    return df_transformed

def main(target_date, price_output_path, nav_output_path):
    """
    Main function to fetch, transform, and save historical price and NAV data.

    Args:
        target_date (str): The date for which to process data (YYYY-MM-DD).
        price_output_path (str): Path to save the transformed price data CSV.
        nav_output_path (str): Path to save the transformed NAV data CSV.
    """
    print("Starting historical data transformation...")
    db_connector = DatabaseConnector("HistoricalPriceDB")
    source_table = "TestHistoricalData" # As specified in the schema

    # Fetch symbol mapping first
    symbol_map_df = fetch_symbol_mapping(db_connector)
    if symbol_map_df.empty:
        print("Could not fetch symbol mapping. Exiting.")
        sys.exit(1)

    # Create sets for efficient lookup
    price_symbols = set(symbol_map_df['Symbol'].unique())
    nav_symbols = set(symbol_map_df['MFQSSym'].unique())

    # Fetch all data for the target date
    raw_df = fetch_historical_data(db_connector, source_table, target_date)

    if raw_df.empty:
        print("No data fetched. Exiting.")
        sys.exit(1)

    # --- Split Data Using Symbol Mapping ---
    # Identify rows where the 'Symbol' column matches a known NAV symbol (MFQSSym)
    nav_mask = raw_df['Symbol'].isin(nav_symbols)
    # Identify rows where the 'Symbol' column matches a known Price symbol (Symbol from CEF_Setup)
    price_mask = raw_df['Symbol'].isin(price_symbols)

    raw_price_data = raw_df[price_mask]
    raw_nav_data = raw_df[nav_mask]

    # Optional: Check for symbols in raw_df not present in either mapping set
    unmapped_symbols = raw_df[~price_mask & ~nav_mask]['Symbol'].unique()
    if len(unmapped_symbols) > 0:
        print(f"Warning: Found {len(unmapped_symbols)} symbols in {source_table} for {target_date} that are not in CEF_Setup mapping:")
        print(unmapped_symbols)

    print(f"Split data using CEF_Setup mapping: {len(raw_price_data)} price records, {len(raw_nav_data)} NAV records.")

    # --- Transform Data ---
    price_df_transformed = transform_price_data(raw_price_data.copy())
    nav_df_transformed = transform_nav_data(raw_nav_data.copy())

    # --- Sort Data ---
    if not price_df_transformed.empty:
        price_df_transformed = price_df_transformed.sort_values(by='Symbol').reset_index(drop=True)
    if not nav_df_transformed.empty:
        nav_df_transformed = nav_df_transformed.sort_values(by='Symbol').reset_index(drop=True)

    # --- Save Data ---
    try:
        price_df_transformed.to_csv(price_output_path, index=False)
        print(f"Transformed price data saved to: {price_output_path}")
    except Exception as e:
        print(f"Error saving price data to {price_output_path}: {e}")

    try:
        nav_df_transformed.to_csv(nav_output_path, index=False)
        print(f"Transformed NAV data saved to: {nav_output_path}")
    except Exception as e:
        print(f"Error saving NAV data to {nav_output_path}: {e}")

    print("Historical data transformation complete.")

# For detailed usage instructions and examples, see ./docs/historical_data_transformer_cli.md
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="""
        Fetches historical price/NAV data from a specified table (e.g., TestHistoricalData)
        for a given date, transforms it into price_df and nav_df formats,
        and saves them as CSV files.
        """)
    parser.add_argument("--date", required=True, help="Target date in YYYY-MM-DD format.")
    parser.add_argument("--price-output", required=True, help="Output file path for transformed price data (CSV).")
    parser.add_argument("--nav-output", required=True, help="Output file path for transformed NAV data (CSV).")

    args = parser.parse_args()

    main(args.date, args.price_output, args.nav_output) 