"""
8/3/24
This is a worthwile project, but I need the data fast, so I'll have to come back
and revisit this.
"""


import sys
import os

# Add the root directory to the Python path
# This assumes your script is one level deep in the project structure
root_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.append(root_dir)

from database_connector_v2 import DatabaseConnector
import pandas as pd
from datetime import datetime, timedelta

def get_price_data(db_connector, end_date):
    # Calculate start dates for different intervals
    start_dates = {
        'week_to_date': end_date - timedelta(days=end_date.weekday()),
        'one_week': end_date - timedelta(weeks=1),
        'one_month': end_date - timedelta(days=30),
        'three_month': end_date - timedelta(days=90),
        'six_month': end_date - timedelta(days=180),
        'one_year': end_date - timedelta(days=365)
    }
    
    # Construct the SQL query
    query = f"""
    SELECT Symbol, Date, ClosePx
    FROM vw_PriceData
    WHERE Date BETWEEN '{min(start_dates.values()).strftime('%Y-%m-%d')}' AND '{end_date.strftime('%Y-%m-%d')}'
    """
    
    # Fetch data using the complex_query method
    results = db_connector.complex_query('vw_PriceData', query)
    
    # Convert results to a pandas DataFrame
    df = pd.DataFrame(results, columns=['Symbol', 'Date', 'ClosePx'])
    return df, start_dates

def calculate_returns(df, end_date, start_dates):
    results = {}
    end_prices = df[df['Date'] == end_date].set_index('Symbol')['ClosePx']
    
    for interval, start_date in start_dates.items():
        start_prices = df[df['Date'] == start_date].set_index('Symbol')['ClosePx']
        returns = (end_prices - start_prices) / start_prices * 100
        results[interval] = returns
    
    return pd.DataFrame(results)

def main():
    # Initialize DatabaseConnector
    db_connector = DatabaseConnector('HistoricalPriceDB')
    
    end_date = datetime.strptime(input("Enter the end date (YYYY-MM-DD): "), '%Y-%m-%d')
    df, start_dates = get_price_data(db_connector, end_date)
    returns = calculate_returns(df, end_date, start_dates)
    print(returns)

if __name__ == "__main__":
    main()