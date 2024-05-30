import os
import pandas as pd
import requests

from config import API_PRODUCT_URL, PRODUCT_IDS, INPUT_COLUMNS


class OpenFoodFactsETL:
    def __init__(self):
        self.product_ids = PRODUCT_IDS
        self.url_template = API_PRODUCT_URL
        self.desired_fields = INPUT_COLUMNS

    def extract(self):
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

    def load(self, df, filename):
        """
        Load the transformed data into a CSV file.

        Parameters:
        df (DataFrame): The pandas DataFrame containing the transformed data.
        filename (str): The name of the file to save the data to.
        """
        # Construct the path to the output directory
        output_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "output"
        )

        # Create the output directory if it does not exist
        os.makedirs(output_dir, exist_ok=True)

        # Construct the full file path
        file_path = os.path.join(output_dir, filename)

        # Save the DataFrame to a CSV file
        df.to_csv(file_path, index=False)

    def run(self, output_filename):
        """
        Run the ETL process: extract, transform, and load data.

        Parameters:
        output_filename (str): The name of the file to save the final data to.
        """
        raw_data = self.extract()
        transformed_data = self.transform(raw_data)
        self.load(transformed_data, output_filename)


if __name__ == "__main__":
    etl = OpenFoodFactsETL()
    etl.run("task1.csv")
