import sys
import logging
from sqlalchemy.sql import text
from sqlalchemy import create_engine, Table, MetaData, select, and_, insert, update, event
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import SQLAlchemyError, NoResultFound, IntegrityError
import os
import pyodbc
import sqlalchemy as sql
import time
from dotenv import load_dotenv
from dataclasses import asdict

# Load environment variables from the .env file
load_dotenv()

# Initialize logger
logger = logging.getLogger(__name__)

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

    ***Struggling with whether or not I should create the 
    The session in SQLAlchemy acts as a staging zone for all the objects loaded into the database session object. 
    Any change made against the objects in the session won't be persisted into the database until you call 
    session.commit(). If something goes wrong, you can revert all changes back to the last commit by calling 
    session.rollback().

    Raw SQL:
    SQLAlchemy allows you to execute raw SQL queries using the Session object.
    
    from sqlalchemy.orm import Session

    with Session(engine) as session:
        results = session.execute("SELECT * FROM table WHERE age BETWEEN 25 AND 35")
        for row in result:
            print(row)

    """

    def __init__(self, db_name, initial_pool_size=5, max_pool_size=20):
        # I'm running into an issue when I want to join tables from different databases.
        # It seems possible that I could make the db_name an optional parameter?
        # The idea would be to create a connection to the server and NOT a specific database
        # if no db_name is provided.

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
        self.engine = create_engine(
            self.database_url,
            pool_size=initial_pool_size,
            max_overflow=max_pool_size - initial_pool_size,
            pool_recycle=3600,
            pool_pre_ping= True,
            pool_timeout=30
        )

        # Set up pool listeners for monitoring
        event.listen(self.engine, 'checkout', self.connection_checkout)

        self.SessionFactory = sessionmaker(bind=self.engine)


        # self.metadata = MetaData(bind=self.engine)
        self.metadata = MetaData()
        # self.metadata.bind = self.engine

        #########################################
        # from GPT4 (Sonnet 3.5 suggest this too.  I'm not sure if it's necessary???)
        self.metadata.reflect(bind=self.engine)
        #########################################

        # self.metadata.reflect()

        # Sonnet3.5: Simplified the creation of self.tables using a dictionary comprehension
        self.tables = {table_name: Table(table_name, self.metadata, autoload_with=self.engine) 
                       for table_name in self.metadata.tables}


    def connection_checkout(self, dbapi_connection, connection_record, connection_proxy):
        self.log_pool_status()


    def log_pool_status(self):
        try:
            status = self.engine.pool.status()
            if isinstance(status, str):
                print(f"Pool Status: {status}")
            else:
                checked_out = getattr(status, 'checkedout', 'N/A')
                available = getattr(status, 'available', 'N/A')
                print(f"Pool Status - Checked out: {checked_out}, Available: {available}")
        except Exception as e:
            print(f"Error getting pool status: {str(e)}")


    def prepare_for_high_frequency(self):
            """Temporarily increase pool size for high-frequency operations"""
            self.engine.dispose()
            self.engine = create_engine(
                self.database_url,
                pool_size=self.engine.pool.size * 2,  # Double the pool size
                max_overflow=self.engine.pool.max_overflow,
                pool_recycle=3600,
                pool_pre_ping=True,
                pool_timeout=10  # Shorter timeout for high-frequency operations
            )
            self.SessionFactory = sessionmaker(bind=self.engine)


    def restore_normal_pool(self):
        """Restore normal pool size after high-frequency operations"""
        self.engine.dispose()
        self.engine = create_engine(
            self.database_url,
            pool_size=5,  # Back to initial size
            max_overflow=15,
            pool_recycle=3600,
            pool_pre_ping=True,
            pool_timeout=30
        )
        self.SessionFactory = sessionmaker(bind=self.engine)


    def read_from_mssql(self, table_name, column_names, filters=None):
        """
        Queries the table and returns the column data specified by the user.

        Args:
            table_name (str): The name of the table to query.
            column_names (list): A list of column names to return.
            filters (list of tuples): Conditions in the form of (column_name, operator, value).

        Filter Map (this is basically the WHERE clause):
            eq: Equal to
            ne: Not equal to
            lt: Less than
            le: Less than or equal to
            gt: Greater than
            ge: Greater than or equal to
            in: In the list
            like: Like a pattern (e.g., 'John%', would match 'John Doe' or 'Johnny')
            between: Between two values (e.g., (10, 20) would match values between 10 and 20)

            To add the equivalent of "WHERE Symbol='NVDA' AND Date='2023-01-01'":
            filters = [('Symbol', 'eq', 'NVDA'), ('Date', 'eq', '2023-01-01')]

        Returns:
            results (list): A list of tuples containing the data from the specified columns.

        """

        logger.info(f"Executing read_from_mssql for table: {table_name}")
        logger.info(f"Columns: {column_names}")
        logger.info(f"Filters: {filters}")

        # Define Metadata and Reflect the table
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        # Create a session for transactions
        # Session = sessionmaker(bind=self.engine)

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

            # This is the equivalent of 'AND'ing all the conditions together!
            stmt = stmt.where(and_(*conditions))

        # Execute the query using a session
        try:
            with self.SessionFactory() as session:
                print('Running read_from_mssql() query...')
                # Wow.  Without fetchall(), "results", when returned to the calling function, isn't usable.
                results = session.execute(stmt).fetchall()
                logger.info(f"Query executed successfully. Retrieved {len(results)} rows.")
                # print(type(results))
                # for row in results:
                #     print(row)
                session.commit()  # Ensure any transactions are committed
                return results
        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            logger.exception("An error occurred during database query:")
            raise


    def complex_query(self, table_name, query): 
        # SQLAlchemy's 'session.execute()' function expecte the raw SQL to be explicitly
        # marked as a textual SQL expression.
        query = text(query)

        try:
            logger.info(f"Executing complex_query: {query} for table: {table_name}")
            with self.SessionFactory() as session:
                logger.info('Session created.  Attempting query..')
                results = session.execute(query).fetchall()
                logger.info(f"Query executed successfully. Retrieved {len(results)} rows.")
                session.commit()  # Ensure any transactions are committed
                return results
        except SQLAlchemyError as e:
            print(f"Error occurred: {e}")
            logger.exception("An error occurred during database query:")
            raise

        ''' 
        Raw SQL:
        SQLAlchemy allows you to execute raw SQL queries using the Session object.
        
        from sqlalchemy.orm import Session

        with Session(engine) as session:
            result = session.execute("SELECT * FROM user WHERE age BETWEEN 25 AND 35")
            for row in result:
                print(row)
        '''


    def upsert_price_record_instances_mssql(self, table_name, dataclass_instances):
        """
        This function expects the data to be instances of an @dataclass.
        """
        print('Running upsert_price_record_instances_mssql()...')

        start_timer = time.time()

        # Define Metadata and Reflect the table
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        # Create a session for transactions
        Session = sessionmaker(bind=self.engine)

        with Session() as session:
            for instance in dataclass_instances:
                instance_dict = asdict(instance)
    
                # Check if the record already exists
                # print('Checking if record exists...')
                sys.stdout.write('\rChecking if record exists...')

                # We are essentially copying our table's composite PK schema to check for record existence.
                match_query = select(table).where(
                    table.c.Symbol == instance_dict['Symbol'], 
                    table.c.Date == instance_dict['Date']
                )

                # Why are we using scalar() here?
                existing_record = session.execute(match_query).scalar()

                if existing_record:
                    # Record exists, perform an update
                    # print(f'Record exists, performing an update on {instance_dict['Symbol']}...')
                    sys.stdout.write(f'\rRecord exists, performing an update on {instance_dict['Symbol']}...')
                    update_stmt = (
                        update(table).
                        where(table.c.Symbol == instance_dict['Symbol'], table.c.Date == instance_dict['Date']).
                        values(instance_dict)
                    )
                    session.execute(update_stmt)
                else:
                    # Record does not exist, perform an insert
                    # print(f'Record does not exist, performing an insert on {instance_dict['Symbol']}...')
                    sys.stdout.write(f'\rRecord does not exist, performing an insert on {instance_dict['Symbol']}...')
                    insert_stmt = insert(table).values(instance_dict)
                    session.execute(insert_stmt)

            session.commit()

        end_timer = time.time()
        print(f"\nUpserted {len(dataclass_instances)} records in: {end_timer - start_timer} seconds.\n")


    def upsert_price_record_mssql_bulk(self, table_name, new_data):
        # THIS CURRENTLY DOES NOT WORK
        # GPT4 IS SAYING I NEED TO DEFINE AN ORM CLASS THAT MAPS TO THE TABLE.
        # GOING TO TEST THE ITERATIVE VERSION TO SEE IF IT'S FAST ENOUGH.
        print('Running upsert_price_record_mssql_bulk()...')

        # Define Metadata and Reflect the table
        table = Table(table_name, self.metadata, autoload_with=self.engine)

        # Create a session for transactions
        Session = sessionmaker(bind=self.engine)

        with Session() as session:
            try:
                # Attempt to bulk insert
                print('Attempting bulk insert...')
                session.bulk_insert_mappings(table, new_data)
                session.commit()
            except IntegrityError:
                # If there's an integrity error, likely due to a duplicate key, perform upsert
                print('Integrity error, performing upsert...')
                session.rollback()
                for data in new_data:
                    stmt = select(table).where(table.c.Symbol == data['Symbol'], table.c.Date == data['Date'])
                    existing_record = session.execute(stmt).fetchone()
                    if existing_record:
                        update_stmt = (
                            update(table).
                            where(table.c.Symbol == data['Symbol'], table.c.Date == data['Date']).
                            values(data)
                        )
                        session.execute(update_stmt)
                    else:
                        session.execute(insert(table).values(data))
                session.commit()