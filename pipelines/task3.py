"""
# Pull the PostgreSQL image
docker pull postgres

# Run a PostgreSQL container
docker run --name postgres-container -e POSTGRES_PASSWORD=admin -d -p 5432:5432 postgres

"""
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float
from sqlalchemy.types import ARRAY
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
        self.table_name = "food_data"

    def save_to_db(self):
        """
        Save the DataFrame to a PostgreSQL database.
        """
        with self.engine.connect() as connection:
            meta = MetaData()
            # Define the table schema with only the primary columns
            columns = [
                Column("product_id", String, primary_key=True),
                Column("product_name", String),
            ]
            # Dynamically add the remaining columns based on DataFrame
            for col in self.df.columns:
                if col not in ["product_id", "product_name"]:
                    if self.df[col].dtype == "float64":
                        columns.append(Column(col, Float))
                    elif self.df[col].dtype == "object":
                        if self.df[col].apply(lambda x: isinstance(x, list)).all():
                            columns.append(Column(col, ARRAY(String)))
                        else:
                            columns.append(Column(col, String))

            food_data_table = Table(
                self.table_name, meta, *columns, extend_existing=True
            )
            meta.create_all(self.engine)

            # Insert data
            self.df.to_sql(
                self.table_name,
                self.engine,
                if_exists="replace",
                index=False,
                dtype={"categories_hierarchy": ARRAY(String)},
            )

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
