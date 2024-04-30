# test_connector.py
from database_connector import DatabaseConnector
import pandas as pd
# from sqlalchemy import text, select, Table, MetaData

# Create an instance of the DatabaseConnector class
# Note: Ensure that your .env file or environment has all the required variables.
db_connector = DatabaseConnector("HistoricalPriceDB")

# Assuming there is a table named 'users' with columns 'id' and 'name'
# and there is at least one user with name 'John Doe'
def test_read():
    # results = db_connector.read("users", name="John Doe")
    # select_stmt = text('select(table.c.Symbol, table.c.MFQSSym, table.c.ClosePx, table.c.NAV)')
    # select_stmt = 'select(table.c.Symbol, table.c.MFQSSym, table.c.ClosePx, table.c.NAV)'
    column_names = ['Symbol', 'MFQSSym', 'ClosePx', 'NAV']
    results = db_connector.read_from_mssql("CEF_price_nav_history", column_names)
    # results = db_connector.read2("CEF_price_nav_history")
    print("Test Read Results:")
    for row in results:
        print(row)


if __name__ == "__main__":
    test_read()
    # read_from_sqlserver()
