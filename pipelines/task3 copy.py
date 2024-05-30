"""
# Pull the PostgreSQL image
docker pull postgres

# Run a PostgreSQL container
docker run --name postgres-container -e POSTGRES_PASSWORD=admin -d -p 5432:5432 postgres

"""
import pandas as pd
from sqlalchemy import insert, create_engine, MetaData, Table, Column, String, Float, Integer, inspect
from sqlalchemy.types import ARRAY
from sqlalchemy.exc import ProgrammingError
import os
import ast

# Import the OpenFoodFactsETL class from task1
from task1 import OpenFoodFactsETL
from config import DB_PWD


class OpenFoodFactsDB:
    def __init__(self, input_filename):
        """
        Initialize the DB class with the input filename and setup database connection.

        Parameters:
        input_filename (str): The name of the input CSV file containing product data.
        """
        self.input_filename = input_filename

        # Construct the path to the input file
        self.input_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "output", input_filename
        )

        # Check if the input file exists
        # Check if the input file exists
        if not os.path.exists(self.input_file_path):
            # If the input file does not exist, run the ETL pipeline from task1
            print(
                f"{self.input_file_path} does not exist. Running ETL pipeline from task1."
            )
            task1 = OpenFoodFactsETL()
            task1.run(input_filename)

        # Read the input file into a pandas DataFrame and ensure 'categories_hierarchy' is parsed as a list
        self.df = pd.read_csv(
            self.input_file_path, converters={"categories_hierarchy": ast.literal_eval}
        )

        # Rename columns
        self.df.rename(
            columns={"id": "product_id", "generic_name": "product_name"}, inplace=True
        )

        # Database connection setup
        self.engine = create_engine(
            f"postgresql+psycopg2://postgres:{DB_PWD}@localhost:5432/postgres"
        )
        self.table_name = "food_data_2"

    def save_to_db(self):
        """
        Save the DataFrame to a PostgreSQL database.
        """
        # Store the DataFrame in the PostgreSQL table
        table_name = 'food_data_2'
        username = 'postgres'
        password = 'admin'
        host = 'localhost'
        port = '5432'  # Default PostgreSQL port
        database = 'postgres'

        # Create the SQLAlchemy engine
        engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')
        # Create a sample DataFrame
        data = {
            'id': [1, 2, 3],
            'array_column': [[1, 2, 3], [4, 5, 6], [7, 8, 9]]
        }
        df = pd.DataFrame(data)
        # Define the table schema using SQLAlchemy
        metadata = MetaData()
        table_name = 'food_data_2'



        # Define the table schema with the correct types
        your_table = Table(
            table_name,
            metadata,
            Column('id', Integer, primary_key=True),
            Column('array_column', ARRAY(Integer)),
            extend_existing=True
        )

        # Create the table in the database
        metadata.create_all(engine)

        # Use the to_sql method with the proper dtype specification for array_column
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype={'array_column': ARRAY(Integer)})

        print(f"DataFrame stored in table {table_name} in PostgreSQL databaseeeee.")

    def run(self):
        """
        Run the process to save data to the database.
        """
        self.save_to_db()


if __name__ == "__main__":
    # Instantiate the DB class with the name of the input CSV file
    db = OpenFoodFactsDB("task1.csv")

    # Run the process to save data to the database
    db.run()

    from sqlalchemy import create_engine, MetaData, Table
    from sqlalchemy.orm import sessionmaker

    # Define the PostgreSQL connection parameters
    username = 'postgres'
    password = 'admin'
    host = 'localhost'
    port = '5432'  # Default PostgreSQL port
    database = 'postgres'

    # Create the SQLAlchemy engine
    engine = create_engine(f'postgresql+psycopg2://{username}:{password}@{host}:{port}/{database}')

    # Create a configured "Session" class
    Session = sessionmaker(bind=engine)

    # Create a Session
    session = Session()

    # Reflect the existing database into a new model
    metadata = MetaData()
    metadata.reflect(bind=engine)

    # Assuming you have a table named 'your_table'
    table_name = 'food_data_2'
    table = Table(table_name, metadata, autoload_with=engine)

    # Print the schema of the table
    print(f"Schema of the table '{table_name}':")
    for column in table.columns:
        print(f"{column.name} ({column.type})")
        print(isinstance(column.type, ARRAY))

    # Execute a query to get all data from the table
    query = session.query(table)

    # Fetch all results
    results = query.all()

    # Print the results
    for row in results:
        pass
        print(row)

    # Close the session
    session.close()
