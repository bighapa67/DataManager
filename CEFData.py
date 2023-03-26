import pandas as pd
import os
import SqlConnection as sc

"""
This program is meant to be used in conjunction with the output from the IQLink Launcher tool provided by
IQFeed to download historical price data.  The output from the IQLink tool is a series of csv files.
The script that uses the IQLink Launcher tool to download the data is in TradingTools and is called 'IQFeed.py'.
"""

raw_data_path = 'F:\PriceData'

# Loop through csv files and extract data
def read_price_data():
    # This data comes from IQFeed.  Unfortunately, IQFeed doesn't seem to be returning MFQS data
    # (like XFFCX, XDFPX, etc.), so I'll have to get that data elsewhere.

    # Get a list of all csv files in the raw data directory
    csv_files = [f for f in os.listdir(raw_data_path) if f.endswith('.csv')]

    # Initialize an empty dataframe to store the data
    df = pd.DataFrame(columns=['Symbol', 'Date', 'High', 'Low', 'Open', 'Close', 'Volume'])
    # df = pd.DataFrame()

    # Iterate through each csv file
    for csv_file in csv_files:
        # Capture the symbol from the csv file name
        symbol = csv_file.split('_')[0]
        print(symbol)

        # Read the csv file into a dataframe, skipping the last column (which is dummy data).
        df_temp = pd.read_csv(raw_data_path + fr'\{csv_file}', header=None, usecols=range(0, 6))

        # Add a column to the beginning of the dataframe that contains the symbol
        df_temp.insert(0, 'Symbol', symbol)
        df_temp.columns = ['Symbol', 'Date', 'High', 'Low', 'Open', 'Close', 'Volume']

        # Convert the date column to a datetime object
        df_temp['Date'] = pd.to_datetime(df_temp['Date'])

        # Zero out the time portion of the datetime column
        df_temp['Date'] = df_temp['Date'].dt.floor('D')
        # print(df_temp.info())

        # Append the data to the summary dataframe.
        df = pd.concat([df, df_temp], ignore_index=True)

    return df


def read_nav_data():
    # Unfortunately, IQFeed doesn't seem to be returning MFQS data (like XFFCX, XDFPX, etc.)
    # I ended up pulling the data from RealTick using my DailyHistoricalPriceDB_WPF program.
    from_sql_df = pd.read_sql(f'SELECT * FROM {sc.db_table}', sc.engine)

    # df = sc.read_from_sqlserver()

    return from_sql_df

# Write the data to a csv file
def save_to_csv(df):
    df.to_csv(r'F:\ResultData' + fr'\CEF_Price_Data.csv', index=False)


if __name__ == '__main__':
    price_df = read_price_data()
    # save_to_csv(price_df)
    nav_df = read_nav_data()

