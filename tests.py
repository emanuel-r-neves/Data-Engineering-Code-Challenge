import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import os
import csv
import shutil
import chispa
from main import export_dataframe_as_parquet_with_partitions, export_dataframe_as_csv, read_csv_file_to_pyspark_dataframe, enriched_data

# Create a Spark session for testing
spark = SparkSession.builder \
    .appName("ChallengeTestApp") \
    .getOrCreate()

def create_mock_data():
    # Create mock data to be used in tests
    data = [
        ("id1", "Product A", "Category X", "6", "2024-12-01", "55.0"),
        ("id2", "Product B", "Category Y", "12", "2024-12-02", "105.0"),
    ]
    columns = ["product_id", "product_name", "category", "quantity", "transaction_date", "price"]

    return spark.createDataFrame(data, columns)

def create_mock_csv(test_path):

    # Define the CSV data
    test_data = [
        ["product_id", "product_name", "category"],
        ["id1", "Product A", "Category X"],
        ["id2", "Product B", "Category Y"]
    ]

    # Ensure the 'tmp' directory exists
    os.makedirs('tmp', exist_ok=True)

    # Write to CSV using the csv module
    with open(test_path, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerows(test_data)

def delete_tmp_files():

    # Manually perform cleanup after running tests
    if os.path.exists('tmp'):
        shutil.rmtree('tmp')
        print("Cleaned up test files.")


def test_read_csv_file():

    # Define path for the CSV test file
    test_path = "tmp/test_products.csv"

    # Create a mock CSV file for testing
    create_mock_csv(test_path)

    # Use the read_csv_file_to_pyspark_dataframe method
    df = read_csv_file_to_pyspark_dataframe(test_path)
    
    # Define the expected DataFrame
    expected_data = [
        ("id1", "Product A", "Category X"),
        ("id2", "Product B", "Category Y")
    ]
    
    expected_columns = ["product_id", "product_name", "category"]

    expected_df = spark.createDataFrame(expected_data, expected_columns)

    # Use Chispa to compare the DataFrames
    chispa.assert_df_equality(df, expected_df, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    # Clean up temporary files
    delete_tmp_files()

def test_export_to_parquet():

    # Create a DataFrame to be written
    df = create_mock_data()

    # Path where the Parquet file will be saved
    output_path = 'tmp/test_parquet_output.parquet'
    
    # Export the DataFrame to a Parquet file
    export_dataframe_as_parquet_with_partitions(df, output_path, partitions_list=["product_name","category"])

    # Read back the Parquet file into a DataFrame
    df_read_back = spark.read.parquet(output_path)

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(df, df_read_back, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True),

    # Clean up temporary files
    delete_tmp_files()

def test_export_to_csv():

    # Create a DataFrame to be written
    df = create_mock_data()

    # Path where the CSV file will be saved
    output_path = 'tmp/test_csv_output.csv'

    # Export the DataFrame to a CSV file
    export_dataframe_as_csv(df, output_path)

    # Read back the CSV file into a DataFrame
    df_read_back = spark.read.option("header", "true").csv(output_path)

    # Compare the DataFrame that was written and the DataFrame read back
    chispa.assert_df_equality(df, df_read_back, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)

    # Clean up temporary files
    delete_tmp_files()

def test_enriched_data():

    # Create sample product data
    df_products = spark.createDataFrame([
        ("p1", "Product A", "Category X"),
        ("p2", "Product B", "Category Y")
    ], ["product_id", "product_name", "category"])

    # Create sample stores data
    df_stores = spark.createDataFrame([
        ("s1", "Store A", "Location X"),
        ("s2", "Store B", "Location Y"),
        ("s3", "Store C", "Location Z")
    ], ["store_id", "store_name", "location"])

    # Create sample sales data
    df_sales = spark.createDataFrame([
        ("t1", "s1", "p1", 5, "2024-12-01", 20.0),
        ("t2", "s2", "p2", 10, "2024-12-02", 40.0),
        ("t3", "s3", "p3", 15, "2024-12-03", 60.0),
        ("t4", "s4", "p4", 20, "2024-12-04", 80.0)
    ], ["transaction_id", "store_id", "product_id", "quantity", "transaction_date", "price"])

    # Enrich data without price category using method
    df_enriched = enriched_data(df_products, df_stores, df_sales, add_price_category=False)

    # Expected enriched result without price category
    expected_df_enriched = spark.createDataFrame([
        ("t1", "Product A", "Category X", "Store A", "Location X", 5, "2024-12-01", 20.0),
        ("t2", "Product B", "Category Y", "Store B", "Location Y", 10, "2024-12-02", 40.0)
    ], ["transaction_id", "product_name", "category", "store_name", "location", "quantity", "transaction_date", "price"])

    # Check that the output of enriched data without price category is correct
    chispa.assert_df_equality(df_enriched, expected_df_enriched, ignore_column_order=True, ignore_nullable=True, ignore_row_order=True)
