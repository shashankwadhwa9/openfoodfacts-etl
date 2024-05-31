"""
docker-compose up -d db

docker-compose run etl
"""
import requests
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table, Column, String, Float, text
from sqlalchemy.types import ARRAY
import os

from config import API_PRODUCT_URL, PRODUCT_IDS, INPUT_COLUMNS


class OpenFoodFactsETL:
    def __init__(self):
        """
        Initialize the OpenFoodFactsETL class and setup database connection.
        """
        self.product_ids = PRODUCT_IDS
        self.url_template = API_PRODUCT_URL
        self.desired_fields = INPUT_COLUMNS

        # Fetch data from the API
        raw_data = self.extract_data()
        self.df = self.transform(raw_data)

        # Rename columns
        self.df.rename(
            columns={"id": "product_id", "generic_name": "product_name"}, inplace=True
        )

        # Database connection setup
        self.engine = create_engine(
            f"postgresql+psycopg2://postgres:{os.getenv('DB_PWD')}@db:5432/postgres"
        )
        self.table_name = "food_data"

    def extract_data(self):
        """
        Extract data from the Open Food Facts API for each product ID.

        Returns:
        list: A list of dictionaries containing product data.
        """
        data = []

        # Iterate over each product ID to fetch data from the API
        for product_id in self.product_ids:
            # Make a GET request to the API for the current product ID
            response = requests.get(self.url_template.format(product_id))

            # Check if the request was successful
            if response.status_code == 200:
                # Parse the JSON response to get the product data
                product_data = response.json().get("product", {})

                # Append the product data to the data list
                data.append(product_data)

        return data

    def transform(self, data):
        """
        Transform the raw data into a pandas DataFrame with only the desired fields.

        Parameters:
        data (list): List of dictionaries containing raw product data.

        Returns:
        DataFrame: A pandas DataFrame containing the transformed data.
        """
        # Normalize JSON data to flat table
        df = pd.json_normalize(data)

        # Keep only desired fields
        df_subset = df[self.desired_fields]

        return df_subset

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
        Run the ETL process to save data to the database.
        """
        self.save_to_db()

    def run_tests(self):
        with self.engine.connect() as connection:
            # Validate the row count
            result = connection.execute(
                text(f"SELECT COUNT(*) FROM {self.table_name};")
            )
            count = result.scalar()
            print(f"Validating that row count is {len(PRODUCT_IDS)}")
            assert count == len(PRODUCT_IDS)

            # Validate the number of columns
            result = connection.execute(
                text(
                    f"SELECT COUNT(*) FROM information_schema.columns WHERE table_name='{self.table_name}';"
                )
            )
            count = result.scalar()
            print(f"Validating that column count is {len(INPUT_COLUMNS)}")
            assert count == len(INPUT_COLUMNS)


if __name__ == "__main__":
    # Instantiate the OpenFoodFactsETL class
    pipeline = OpenFoodFactsETL()

    # Run the ETL process to save data to the database
    pipeline.run()

    # Verify the results
    pipeline.run_tests()
