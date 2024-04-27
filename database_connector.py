from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError
import os
import pyodbc
import sqlalchemy as sql
from dotenv import load_dotenv

# Load environment variables from the .env file
load_dotenv()

class DatabaseConnector:

    """
    This class requires a fully formed database_url (connection string) to connect to a Sql Server database.
    Since the only thing that changes from one call to the next are the database name and the table name,
    I'm going to parameterize those values in the constructor while obtainint the rest of the connection
    string from the .env file.

    SQLAlchemy Connections (from GPT4):
    SQLAlchemy uses a connection pool that automatically manages multiple connections to your database. 
    The create_engine() function initializes this pool. The engine maintains a pool of connections that are 
    kept open and reused, rather than opening and closing a connection for every database operation. 
    This enhances performance by reducing the overhead of repeatedly establishing connections to the database.

    Sessions:
    The session in SQLAlchemy acts as a staging zone for all the objects loaded into the database session object. 
    Any change made against the objects in the session won't be persisted into the database until you call 
    session.commit(). If something goes wrong, you can revert all changes back to the last commit by calling 
    session.rollback().
    """

    def __init__(self, db_name):
        # Retrieve environment variables
        db_user = os.getenv("DB_USER")
        db_pswd = os.getenv("DB_PSWD")
        db_host = os.getenv("DB_HOST")
        db_port = os.getenv("DB_PORT")
        db_driver = os.getenv("DB_DRIVER")
        
        # Validate that all necessary environment variables are present
        if not all([db_user, db_pswd, db_host, db_port, db_driver, db_name]):
            raise EnvironmentError("One or more environment variables are missing.")

        # Construct the database_url and initialize the engine, session and metadata.
        self.database_url = f"mssql+pyodbc://{db_user}:{db_pswd}@{db_host}:{db_port}/{db_name}?driver={db_driver}"
        self.engine = create_engine(self.database_url)
        self.Session = sessionmaker(bind=self.engine)
        # self.metadata = MetaData(bind=self.engine)
        self.metadata = MetaData()
        self.metadata.bind = self.engine


    def read(self, table_name, **filters):
        # table = Table(table_name, self.metadata, autoload_with=self.engine)
        table = Table(table_name, self.metadata, self.engine)
        try:
            with self.Session() as session:
                result = session.execute(table.select().where_by(**filters))
                return [dict(row) for row in result]
        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
