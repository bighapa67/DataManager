from sqlalchemy import create_engine, Table, MetaData, select, and_, insert, update
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, NoResultFound
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
    I'm going to parameterize those values in the constructor while obtaining the rest of the connection
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

    Raw SQL:
    SQLAlchemy allows you to execute raw SQL queries using the Session object.
    
    from sqlalchemy.orm import Session

    with Session(engine) as session:
        result = session.execute("SELECT * FROM user WHERE age BETWEEN 25 AND 35")
        for row in result:
            print(row)

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
                                # "mssql+pyodbc://Ken:Skando!23Q@localhost:1433/HistoricalPriceDB?driver=ODBC Driver 17 for Sql Server"
        # self.database_url = os.getenv("DATABASE_URL")
        self.engine = create_engine(self.database_url)
        # self.Session = sessionmaker(bind=self.engine)
        # self.metadata = MetaData(bind=self.engine)
        self.metadata = MetaData()
        # self.metadata.bind = self.engine
        # from GPT4
        # self.metadata.reflect(bind=self.engine)
        # self.metadata.reflect()

    def read_from_mssql(self, table_name, column_names, filters=None):
        """
        Queries the table and returns the column data specified by the user.

        Args:
            table_name (str): The name of the table to query.
            column_names (list): A list of column names to return.
            filters (list of tuples): Conditions in the form of (column_name, operator, value).

        Filter Map:
            eq: Equal to
            ne: Not equal to
            lt: Less than
            le: Less than or equal to
            gt: Greater than
            ge: Greater than or equal to
            in: In the list
            like: Like a pattern (e.g., 'John%', would match 'John Doe' or 'Johnny')
            between: Between two values (e.g., (10, 20) would match values between 10 and 20)

        Returns:
            results (list): A list of tuples containing the data from the specified columns.

        """

        # Define Metadata and Reflect the table
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        # Create a session for transactions
        Session = sessionmaker(bind=self.engine)

        # Query the DB with a Select stmt
        # Generate the column list for the select statement
        columns_to_select = [getattr(table.c, column_name) for column_name in column_names]

        # Create a select statement with the specified columns
        stmt = select(*columns_to_select)

        # Add filters to the query
        if filters:
            # Apply filter conditions dynamically based on tuple definitions
            conditions = []
            for column_name, op, value in filters:
                column = getattr(table.c, column_name)
                if op == 'eq':
                    conditions.append(column == value)
                elif op == 'ne':
                    conditions.append(column != value)
                elif op == 'lt':
                    conditions.append(column < value)
                elif op == 'le':
                    conditions.append(column <= value)
                elif op == 'gt':
                    conditions.append(column > value)
                elif op == 'ge':
                    conditions.append(column >= value)
                elif op == 'in':
                    conditions.append(column.in_(value))
                elif op == 'like':
                    conditions.append(column.like(value))
                elif op == 'between':
                    # Ensure value is a tuple or list with exactly two elements
                    if isinstance(value, (list, tuple)) and len(value) == 2:
                        conditions.append(column.between(value[0], value[1]))

            stmt = stmt.where(and_(*conditions))

        # Execute the query using a session
        try:
            with Session() as session:
                # Wow.  Without fetchall(), "results", when returned to the calling function, isn't usable.
                results = session.execute(stmt).fetchall()
                print('Running read_from_mssql() query...')
                # print(type(results))
                # for row in results:
                #     print(row)
                session.commit()  # Ensure any transactions are committed
                return results
        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            return None
    

    def upsert_price_record_mssql(self, table_name, new_data):
        print('Running upsert_to_mssql()...')

        # Define Metadata and Reflect the table
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        # Create a session for transactions
        Session = sessionmaker(bind=self.engine)

        with Session() as session:
            # Check if the record already exists
            print('Checking if record exists...')
            match_query = select(table).where(table.c.Symbol == new_data['Symbol'], table.c.Date == new_data['Date'])
            result = session.execute(match_query).fetchone()

            if result:
                # Record exists, perform an update
                print('Record exists, performing an update...')
                update_stmt = (
                    update(table).
                    where(table.c.Symbol == new_data['Symbol'], table.c.Date == new_data['Date']).
                    values(new_data)
                )
                session.execute(update_stmt)
            else:
                # Record does not exist, perform an insert
                print('Record does not exist, performing an insert...')
                insert_stmt = insert(table).values(new_data)
                session.execute(insert_stmt)

            session.commit()

            # try:
            #     existing_record = session.query(table).filter(table.c.Symbol == new_data['Symbol']).one()
            #     # Update existing user
            #     for key, value in table.items():
            #         setattr(existing_record, key, value)
            # except NoResultFound:
            #     # Insert new user
            #     new_record = table(**new_data)
            #     session.add(new_record)

            # session.commit()
