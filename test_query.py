# test_connector.py
from database_connector import DatabaseConnector
import pandas as pd
# from sqlalchemy import text, select, Table, MetaData

# Create an instance of the DatabaseConnector class
# Note: Ensure that your .env file or environment has all the required variables.
HistoricalPriceDB_conn = DatabaseConnector("HistoricalPriceDB")

# Assuming there is a table named 'users' with columns 'id' and 'name'
# and there is at least one user with name 'John Doe'
def test_read():
    # results = db_connector.read("users", name="John Doe")
    # select_stmt = text('select(table.c.Symbol, table.c.MFQSSym, table.c.ClosePx, table.c.NAV)')
    # select_stmt = 'select(table.c.Symbol, table.c.MFQSSym, table.c.ClosePx, table.c.NAV)'
    column_names = ['Symbol', 'Date', 'MFQSSym', 'ClosePx', 'NAV']
    filters = [('Date', 'gt', '2024-04-18')]
    results = HistoricalPriceDB_conn.read_from_mssql("CEF_price_nav_history", column_names, filters)

    results_df = pd.DataFrame(results)
    print(results_df)
    print()
    # results = db_connector.read2("CEF_price_nav_history")
    print("Test Read Results:")
    for row in results:
        print(row)

def test_upsert():
    # Define the table name and the new data to upsert
    new_data = [{
        'Symbol': 'TEST',
        'Date': '2024-05-04',
        'MFQSSym': 'XTESTX',
        'OpenPx': 70.00,
        'HighPx': 95.00,
        'LowPx': 65.00,
        'ClosePx': 74.69,
        'NAV': 120.00
    }, {
        'Symbol': 'TEST',
        'Date': '2024-05-05',
        'MFQSSym': 'XTESTX',
        'OpenPx': 70.00,
        'HighPx': 95.00,
        'LowPx': 65.00,
        'ClosePx': 75.69,
        'NAV': 120.00
    }]

    # Upsert the new data to the table
    HistoricalPriceDB_conn.upsert_price_record_mssql2("CEF_price_nav_history", new_data)


if __name__ == "__main__":
    # test_read()
    test_upsert()
