import pandas as pd
import os
import urllib
import SqlConnection as sc
import sqlalchemy as sql
from sqlalchemy.orm import sessionmaker

"""
This program is meant to be the conductor that orchestrates the capture and recording of my CEF related data.
The main components are the price and NAV of my closed-end funds.
Dividends are also of vital concern, but for now I'm going to separate that process out.
"""

# environment variables
os.environ['MSSQL_DB'] = 'HistoricalPriceDB'
os.environ['MSSQL_TABLE'] = 'CEF_price_nav_history'

sc.database = os.environ['MSSQL_DB']
sc.db_table = os.environ['MSSQL_TABLE']

symbol_source_df  = pd.DataFrame()


test_df = pd.DataFrame()
test_df = sc.read_from_sqlserver()
print(test_df)

def load_symbols():
    """
    This function is meant to load the symbols of the CEFs that I want to track.
    :return: a list of symbols
    """
    symbols = 