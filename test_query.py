# test_connector.py
from database_connector import DatabaseConnector
import pandas as pd

# Create an instance of the DatabaseConnector class
# Note: Ensure that your .env file or environment has all the required variables.
db_connector = DatabaseConnector("HistoricalPriceDB")

# Assuming there is a table named 'users' with columns 'id' and 'name'
# and there is at least one user with name 'John Doe'
def test_read():
    # results = db_connector.read("users", name="John Doe")
    results = db_connector.read("CEF_price_nav_history")
    print("Test Read Results:")
    for row in results:
        print(row)

def read_from_sqlserver():
    from_sql_df = pd.read_sql(f'SELECT * FROM {db_table}', engine)
    return from_sql_df


if __name__ == "__main__":
    # test_read()
    read_from_sqlserver()
