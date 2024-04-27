import os
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import pandas as pd
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()
db_name = 'HistoricalPriceDB'

# Assuming DATABASE_URL is defined in your environment variables or define it directly
# database_url = os.getenv("DATABASE_URL", "mssql+pyodbc://Ken:Skando!23Q@localhost:1433/FRANKENSTEIN?driver=ODBC Driver 17 for Sql Server")
# database_url = "mssql+pyodbc://Ken:Skando!23Q@localhost:1433/HistoricalPriceDB?driver=ODBC Driver 17 for Sql Server"
db_user = os.getenv("DB_USER")
db_pswd = os.getenv("DB_PSWD")
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_driver = os.getenv("DB_DRIVER")
database_url = f"mssql+pyodbc://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_name}?driver={db_driver}"

# database_url = os.getenv("DATABASE_URL")

# Create an engine
engine = create_engine(database_url)

try:
    # Connect to the database
    connection = engine.connect()
    print("Successfully connected to the database.")

    # result_df = pd.DataFrame()
    
    # Execute a simple query (e.g., selecting the current date/time)
    # result_df = pd.DataFrame(connection.execute(text("SELECT * FROM HistoricalPriceDB.dbo.CEF_price_nav_history")))
    result = connection.execute(text("SELECT * FROM CEF_price_nav_history"))
    result_df = pd.DataFrame(result)

    # for row in result:
    #     print("Current Date and Time:", row[0])
    # print(result.fetchall())
    print(result_df)

    # Close the connection
    connection.close()
except SQLAlchemyError as e:
    print("Error occurred during database connection:", str(e))

# Optional: Test with a specific SQL query that reflects your use case
