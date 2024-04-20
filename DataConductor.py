import pandas as pd
import os
import urllib
import SqlConnection as sc

"""
This program is meant to be the conductor that orchestrates the capture and recording of my CEF related data.
The main components are the price and NAV of my closed-end funds.
Dividends are also of vital concern, but for now I'm going to separate that process out.
"""

# environment variables
os.environ['MSSQL_DB'] = 'HistoricalPriceDB'
os.environ['MSSQL_TABLE'] = 'CEF_price_nav_history'

# sc.database = os.environ['MSSQL_DB']
# sc.db_table = os.environ['MSSQL_TABLE']
# sc.server = r'FRANKENSTEIN\SQLEXPRESS'
# sc.params = urllib.parse.quote_plus('DRIVER={ODBC Driver 17 for Sql Server};SERVER='+sc.server+';DATABASE='
#                                     +sc.database+";Trusted_Connection=yes;")

test_df = pd.DataFrame()
test_df = sc.read_from_sqlserver()
print(test_df)
