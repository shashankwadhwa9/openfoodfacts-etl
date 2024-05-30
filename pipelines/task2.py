import pandas as pd
import os
import ast

# Import the OpenFoodFactsETL class from task1
from task1 import OpenFoodFactsETL


class OpenFoodFactsAnalysis:
    def __init__(self, input_filename):
        """
        Initialize the analysis class with the input filename.

        Parameters:
        input_filename (str): The name of the input CSV file containing product data.
        """
        self.input_filename = input_filename

        # Construct the path to the input file
        self.input_file_path = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "output", input_filename
        )

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
            self.input_file_path,
            converters={"categories_hierarchy": ast.literal_eval},
        )

    def count_products_by_category(self):
        """
        Count the number of products in each category.

        Returns:
        DataFrame: A pandas DataFrame with columns 'category' and 'count'.
        """
        # Explode the categories_hierarchy column to separate rows for each category
        exploded_df = self.df.explode("categories_hierarchy")

        # Group by categories_hierarchy and count the occurrences
        category_count_df = (
            exploded_df["categories_hierarchy"].value_counts().reset_index()
        )
        category_count_df.columns = ["category", "count"]
        return category_count_df

    def find_min_max_nutriscore_categories(self, category_count_df):
        """
        Find the categories with the lowest and highest nutriscore_score.

        Parameters:
        category_count_df (DataFrame): A DataFrame with category counts.

        Returns:
        DataFrame: A pandas DataFrame with columns 'category_name_min' and 'category_name_max'.
        """
        # Group by categories_hierarchy and calculate the mean nutriscore_score for each category
        category_nutriscore_df = (
            self.df.explode("categories_hierarchy")
            .groupby("categories_hierarchy")["nutriscore_score"]
            .mean()
            .reset_index()
        )

        # Find the category with the minimum nutriscore_score
        min_nutriscore_category = category_nutriscore_df.loc[
            category_nutriscore_df["nutriscore_score"].idxmin()
        ]["categories_hierarchy"]

        # Find the category with the maximum nutriscore_score
        max_nutriscore_category = category_nutriscore_df.loc[
            category_nutriscore_df["nutriscore_score"].idxmax()
        ]["categories_hierarchy"]

        # Combine the results into a DataFrame
        min_max_df = pd.DataFrame(
            {
                "category_name_min": [min_nutriscore_category],
                "category_name_max": [max_nutriscore_category],
            }
        )
        return min_max_df

    def find_products_with_extreme_nutriscores(self):
        """
        Find the products with the maximum and minimum nutriscore_score.

        Returns:
        tuple: Names of the products with the minimum and maximum nutriscore_score.
        """
        # Find the product with the minimum nutriscore_score
        min_nutriscore_product = self.df.loc[self.df["nutriscore_score"].idxmin()][
            "generic_name"
        ]

        # Find the product with the maximum nutriscore_score
        max_nutriscore_product = self.df.loc[self.df["nutriscore_score"].idxmax()][
            "generic_name"
        ]

        return min_nutriscore_product, max_nutriscore_product

    def replace_missing_origins(self, replacement_value="Unknown"):
        """
        Replace missing values in the 'origins' column with the specified replacement value.

        Parameters:
        replacement_value (str): The value to replace missing values with.

        Returns:
        DataFrame: The DataFrame with missing values in 'origins' replaced.
        """
        self.df["origins"].fillna(replacement_value, inplace=True)
        return self.df

    def save_to_csv(self, df, filename):
        """
        Save the DataFrame to a CSV file.

        Parameters:
        df (DataFrame): The DataFrame to save.
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

    def run_analysis(self):
        """
        Run the data analysis tasks and save the results.
        """
        # Task a) Count products by category
        category_count_df = self.count_products_by_category()
        self.save_to_csv(category_count_df, "task2a_category_count.csv")

        # Task b) Find categories with the lowest and highest nutriscore_score
        min_max_df = self.find_min_max_nutriscore_categories(category_count_df)
        self.save_to_csv(min_max_df, "task2b_min_max_nutriscore_categories.csv")

        # Task c) Find products with the maximum and minimum nutriscore_score
        (
            min_nutriscore_product,
            max_nutriscore_product,
        ) = self.find_products_with_extreme_nutriscores()
        print(f"Product with minimum nutriscore: {min_nutriscore_product}")
        print(f"Product with maximum nutriscore: {max_nutriscore_product}")

        # Save the products with extreme nutriscores to a CSV
        extreme_nutriscores_df = pd.DataFrame(
            {
                "extreme": ["min", "max"],
                "product": [min_nutriscore_product, max_nutriscore_product],
            }
        )
        self.save_to_csv(extreme_nutriscores_df, "task2c_extreme_nutriscores.csv")

        # Task d) Replace missing values in 'origins'
        df_with_replaced_origins = self.replace_missing_origins()
        self.save_to_csv(
            df_with_replaced_origins, "task2d_products_with_replaced_origins.csv"
        )


if __name__ == "__main__":
    # Instantiate the analysis class with the name of the input CSV file
    analysis = OpenFoodFactsAnalysis("task1.csv")

    # Run the analysis and save the results
    analysis.run_analysis()
