# test_connector.py
from database_connector import DatabaseConnector
import pandas as pd
from table_schemas.CEF_price_nav_history import CEF_price_nav_history
from dataclasses import asdict, fields
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

def create_data():
    # Define the table name and the new data to upsert
    # new_data = {
    #     'Symbol': 'TEST',
    #     'Date': '2024-01-01',
    #     'MFQSSym': 'XTESTX',
    #     'OpenPx': 70.00,
    #     'HighPx': 95.00,
    #     'LowPx': 65.00,
    #     'ClosePx': 74.69,
    #     'NAV': 120.00,
    # }

    new_data = {
        'MFQSSym': 'XDDDX',
        'OpenPx': 70.00,
        'HighPx': 95.00,
        'LowPx': 65.00,
        'ClosePx': 74.69,
        'NAV': 120.00,
        'Symbol': 'DDD',
        'Date': '2024-02-02',
        'Bullshit': 'Bullshit'
    }

    # new_data = [{
    #     'Symbol': 'TEST',
    #     'Date': '2024-05-05',
    #     'MFQSSym': 'XTESTX',
    #     'OpenPx': 70.00,
    #     'HighPx': 95.00,
    #     'LowPx': 65.00,
    #     'ClosePx': 74.69,
    #     'NAV': 120.00
    # }, {
    #     'Symbol': 'TEST',
    #     'Date': '2024-05-05',
    #     'MFQSSym': 'XTESTX',
    #     'OpenPx': 70.00,
    #     'HighPx': 95.00,
    #     'LowPx': 65.00,
    #     'ClosePx': 75.69,
    #     'NAV': 120.00
    # }]

    # new_data = {
    # 'ExtraColumn1': [1, 2, 3],
    # 'Symbol': ['XYZ', 'ABC', 'DDD'],
    # 'Date': ['2024-01-01', '2024-01-02', '2024-02-01'],
    # 'MFQSSym': ['X1', 'X2', 'X3'],
    # 'ExtraColumn2': [3, 4, 5],
    # 'OpenPx': [100.0, 102.0, 104.0],
    # 'HighPx': [110.0, 112.0, 114.0],
    # 'LowPx': [90.0, 92.0, 94.0],
    # 'ClosePx': [105.0, 107.0, 109.0],
    # 'NAV': [120.0, 122.0, 124.0]
    # }
    
    df = pd.DataFrame(new_data)

    # Incredible GPT4 technique leveraging dataclasses.
    # We use list comprehension to grab the names of the fields we require to satisfy our table schema.
    # Then we filter the dataframe to only include those columns!!!
    required_columns = [field.name for field in fields(CEF_price_nav_history)]
    filtered_df = df[required_columns]

    print(filtered_df)

    return filtered_df

    # Now we need to convert each row of the df into an instance of our data class.


    # data_df = pd.DataFrame(new_data)
    # print(data_df)


    # Upsert the new data to the table
    # HistoricalPriceDB_conn.upsert_single_price_record_mssql("CEF_price_nav_history", new_data)

def create_df_instances(row):
    return CEF_price_nav_history(**row)


if __name__ == "__main__":
    # test_read()
    filtered_df = create_data()
    data_class_instances = filtered_df.apply(create_df_instances, axis=1)

    HistoricalPriceDB_conn.upsert_price_record_instances_mssql("CEF_price_nav_history", data_class_instances)
    
    # for instance in data_class_instances:
    #     print(instance)
    #     HistoricalPriceDB_conn.upsert_single_price_record_mssql2("CEF_price_nav_history", instance)

    # breakpoint
