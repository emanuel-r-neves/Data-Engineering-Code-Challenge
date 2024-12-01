import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DecimalType, DateType
import shutil
import os
import glob

# Set up logging configuration
logging.basicConfig(
    level=logging.INFO,  # Logging level can be adjusted based on necessity (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format="%(asctime)s - %(levelname)s - %(message)s",  # Log message format
)
logger = logging.getLogger(__name__)

# Create a Spark session
spark = SparkSession.builder \
    .appName("ChallengeApp") \
    .getOrCreate()

# Preparation

# Task: Part 1

# Load CSV files into pyspark DataFrame
def read_csv_file_to_pyspark_dataframe(input_path):
    """
    Loads a CSV file from the specified input path into a PySpark DataFrame.
    
    Arguments:
    input_path (str): Path to the CSV file.

    Returns:
    DataFrame: Loaded DataFrame.
    """

    logger.info(f"Reading CSV file from {input_path}")

    df = spark.read.format("csv") \
        .option("header", "true") \
        .option("sep", ",") \
        .load(input_path)
    
    logger.info(f"Loaded CSV file from {input_path} with {df.count()} rows.")

    return df

# Data validation

# Schema validation
def validate_schema(df, expected_schema):
    """
    Validates whether the schema of a DataFrame matches the expected schema, ignoring the order of columns.

    Arguments:
        df (DataFrame): The PySpark DataFrame whose schema is to be validated.
        expected_schema (StructType): The expected schema that the DataFrame should comply with.

    Returns:
    None: The function uses logging to notify if the validation was successfull.
    """
    
    logger.info("Validating schema...")

    # Compare the schema of the DataFrame with the expected schema, ignoring order
    df_fields = set((field.name, field.dataType) for field in df.schema.fields)
    expected_fields = set((field.name, field.dataType) for field in expected_schema.fields)

    if df_fields == expected_fields:
        logger.info("Schemas match.")
    else:
        logger.critical("Schemas match (order doesn't matter)!")

# Products data validation
def validate_products_data(df_products):
    """
    Validates the data types for the product-related columns in the DataFrame.
    This function checks whether the columns in the input DataFrame match the expected schema 
    for product data.
    Additionally, removes duplicate records based on the 'product_id' column.

    Arguments:
    df_products (DataFrame): Input products DataFrame.

    Returns:
    DataFrame: The input DataFrame after schema validation.
    """

    logger.info("Validating product data...")


    # Expected schema specification
    expected_schema = StructType([
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True)
    ])

    validate_schema(df_products, expected_schema)
    # Remove duplicates (if any) based on product_id
    df_products = df_products.dropDuplicates(["product_id"])

    logger.info(f"Finished validated product data.")

    return df_products

# Sales data validation
def validate_sales_data(df_sales):
    """
    Validates and casts the data types for non-String sales data columns (quantity, price, transaction_date).
    Filters out records with null values on quantity, price and transaction_date fields after validation.
    Additionally, removes duplicate records based on the 'transaction_id' column.
    
    Arguments:
    df_sales (DataFrame): Input sales DataFrame.

    Returns:
    DataFrame: DataFrame with validated data.
    """

    logger.info("Validating sales data...")


    # Enforcing data types on raw data before they can be treated
    df_sales_validated = df_sales.withColumn('quantity', F.col('quantity').cast('int')) \
            .withColumn('price', F.col('price').cast(DecimalType(10,2))) \
            .withColumn('transaction_date', \
                F.coalesce( \
                    F.to_date('transaction_date', 'MMMM d, yyyy'), \
                    F.to_date('transaction_date', 'yyyy-MM-dd'), \
                    F.to_date('transaction_date', 'yyyy/MM/dd'), \
                    F.to_date('transaction_date', 'dd-MM-yyyy'), \
                    F.to_date('transaction_date', 'MM/dd/yyyy')))
    
    # Records with null values
    df_sales_invalid_quantity = df_sales_validated.filter(F.col('quantity').isNull())
    df_sales_invalid_price = df_sales_validated.filter(F.col('price').isNull())
    df_sales_invalid_transaction_date = df_sales_validated.filter(F.col('transaction_date').isNull())

    # Filter out records with null values (if any)
    df_sales_validated = df_sales_validated.filter(F.col('quantity').isNotNull()) \
                                            .filter(F.col('price').isNotNull()) \
                                            .filter(F.col('transaction_date').isNotNull())
    
    # Remove duplicates (if any) based on transaction_id
    df_sales_validated = df_sales_validated.dropDuplicates(["transaction_id"])

    # Expected schema specification
    expected_schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("store_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("transaction_date", DateType(), True),
        StructField("price", DecimalType(10, 2), True)
    ])

    validate_schema(df_sales_validated, expected_schema)

    logger.info(f"Finished validating sales data.")

    return df_sales_validated

# Stores data validation
def validate_stores_data(df_stores):
    """
    Validates the data types for the store-related columns in the DataFrame.
    This function checks whether the columns in the input DataFrame match the expected schema 
    for store data.
    Additionally, removes duplicate records based on the 'store_id' column.

    Arguments:
    df_stores (DataFrame): Input stores DataFrame.

    Returns:
    DataFrame: The input DataFrame after schema validation.
    """

    logger.info("Validating stores data...")

    # Expected schema specification
    expected_schema = StructType([
        StructField("store_id", StringType(), True),
        StructField("store_name", StringType(), True),
        StructField("location", StringType(), True)
    ])

    validate_schema(df_stores, expected_schema)

    # Remove duplicates (if any) based on store_id
    df_stores = df_stores.dropDuplicates(["store_id"])

    logger.info(f"Finished validating stores data.")

    return df_stores

# Task: Part 2

# Sales aggregation
def aggregate_sales_amount_store_category(df_products, df_sales):
    """
    Aggregates sales data by store and category, calculating total revenue per store/category.
    Filters out records with null values on category field on the final DataFrame.

    Arguments:
    df_products (DataFrame): Product DataFrame containing category information.
    df_sales (DataFrame): Sales DataFrame.

    Returns:
    DataFrame: Aggregated sales data with total revenue per store/category.
    """

    logger.info(f"Processing aggregated sales amount for each store/category...")


    df_products_category = df_products.select('product_id','category')
    df_sales_aggregation = df_sales.join(df_products_category, 'product_id','left')
    df_sales_total_revenue = df_sales_aggregation.select('store_id', 'category', 'price') \
                                                .groupBy('store_id','category') \
                                                .agg(F.sum('price') \
                                                .alias('total_revenue'))
    
    df_sales_total_revenue = df_sales_total_revenue.filter(F.col('category').isNotNull())

    logger.info(f"Finished processing aggregated sales amount for each store/category.")

    #df_sales_total_revenue.show()
    return df_sales_total_revenue

# Monthly sales insights
def monthly_sales_quantity_insights(df_products, df_sales):
    """
    Calculates monthly sales quantity insights by category.
    Filters out records with null values on category field on the final DataFrame.

    Arguments:
    df_products (DataFrame): Product DataFrame containing category information.
    df_sales (DataFrame): Validated sales DataFrame.

    Returns:
    DataFrame: Aggregated sales data with total quantity sold per month/category.
    """

    logger.info(f"Processing monthly sales quantity insights for each category...")

    df_products_category = df_products.select('product_id','category')
    df_sales_aggregation = df_sales.join(df_products_category, 'product_id','left')
    df_monthly_sales_insights = df_sales_aggregation.withColumn('year', F.year('transaction_date')) \
                                                .withColumn('month', F.month('transaction_date')) \
                                                .select('year','month','category','quantity') \
                                                .groupBy('year','month','category') \
                                                .agg(F.sum('quantity') \
                                                .alias('total_quantity_sold'))
    

    df_monthly_sales_insights = df_monthly_sales_insights.filter(F.col('category').isNotNull())
    #df_monthly_sales_insights.show()

    logger.info(f"Finished processing monthly sales quantity insights for each category.")

    return df_monthly_sales_insights

# Price categorization
def categorize_price(price):
    """
    Categorizes the price into 'Low', 'Medium', or 'High' categories based on a rule.

    Arguments:
    price (Decimal): The price of a product.

    Returns:
    str: Price category ('Low', 'Medium', or 'High').
    """

    logger.info(f"Processing price category...")

    if price < 20:
        return "Low"
    elif 20 <= price <= 100:
        return "Medium"
    else:
        return "High"

# Enriched data
def enriched_data(df_products, df_stores, df_sales, add_price_category):
    """
    Enriches the sales data with product and store details. Optionally adds price category.
    Filters out records with null values on category, product_name, location and store_name fields on the final DataFrame.

    Arguments:
    df_products (DataFrame): Product DataFrame.
    df_stores (DataFrame): Store DataFrame.
    df_sales (DataFrame): Validated sales DataFrame.
    add_price_category (bool): Whether to add a 'price_category' column based on price.

    Returns:
    DataFrame: Enriched sales data with optional price categorization.
    """

    logger.info(f"Processing enriched sales data with product and store details.")


    df_enriched_data = df_sales.join(df_products, 'product_id','left').join(df_stores, 'store_id','left')
    df_enriched_data = df_enriched_data.select('transaction_id', 'store_name', 'location', 'product_name', 'category', 'quantity', 'transaction_date', 'price')
    #df_enriched_data.show()

    if add_price_category:
        df_enriched_data = df_enriched_data.withColumn("price_category", F.udf(categorize_price, StringType())("price"))

    #df_enriched_data.show()

    df_enriched_data = df_enriched_data.filter(F.col('category').isNotNull()) \
                                .filter(F.col('product_name').isNotNull()) \
                                .filter(F.col('location').isNotNull()) \
                                .filter(F.col('store_name').isNotNull())

    logger.info(f"Finished processing enriched sales data with product and store details.")

    return df_enriched_data

# Task: Part 3

# Export enriched DataFrame in a partitioned Parquet file
def export_dataframe_as_parquet_with_partitions(df, output_path, partitions_list):
    """
    Exports a DataFrame to a Parquet file, partitioned by the specified columns.

    Arguments:
    df (DataFrame): The DataFrame to export.
    output_path (str): Path where the Parquet file will be saved.
    partitions_list (list): List of column names by which to partition the data.

    Returns:
    None
    """

    logger.info(f"Exporting DataFrame to Parquet format located at {output_path}, partitioned by {', '.join(partitions_list)}.")

    df.write \
    .mode("overwrite") \
    .partitionBy(*partitions_list) \
    .parquet(output_path)

    logger.info(f"Finished exporting data.")

# Export store revenue insights DataFrame in CSV format
def export_dataframe_as_csv(df, output_path):
    """
    Export a DataFrame to a CSV file.

    Arguments:
    df (DataFrame): The DataFrame to export.
    output_path (str): Path where the CSV file will be saved.

    Returns:
    None
    """
    logger.info(f"Exporting DataFrame to CSV format located at {output_path}.")


    # Temporary path to CSV files
    temp_folder = output_path.replace(".csv","_temp")
    # Writing DataFrame to a designated temporary folder
    df.coalesce(1).write.option("header", "true").mode("overwrite").csv(temp_folder)
    # Get the path to the generated part file based on pattern "part-00000*"
    part_file = glob.glob(os.path.join(temp_folder, "part-00000*"))[0]
    # Rename the partition file to the desired output file name
    os.rename(part_file, output_path)
    # Remove the temporary folder
    shutil.rmtree(temp_folder)

    logger.info(f"Finished exporting data.")


# Execution

# Task: Part 1 - Data Preparation

# 1.1 Import data
# 1.1.1 Import product data
df_products = read_csv_file_to_pyspark_dataframe("data/input/products_uuid.csv")
#df_products.show()

# 1.1.2 Import sales data
df_sales = read_csv_file_to_pyspark_dataframe("data/input/sales_uuid.csv")
#df_sales.show()

# 1.1.3 Import transactions data
df_stores = read_csv_file_to_pyspark_dataframe("data/input/stores_uuid.csv")
#df_stores.show()

# 1.2.1 Products data validation
df_products_validated = validate_products_data(df_products)
# 1.2.2 Sales data validation
df_sales_validated = validate_sales_data(df_sales)
# 1.2.3 Stores data validation
df_stores_validated = validate_stores_data(df_stores)

# Task: Part 2 - Data Transformation

# 2.1. Sales Aggregation - Calculate the total revenue for each store and each product category.
df_aggregated_sales = aggregate_sales_amount_store_category(df_products_validated, df_sales_validated)

# 2.2. Monthly Sales Insights - Calculate the total quantity sold for each product category, grouped by month
df_monthly_sales_quantity_insights = monthly_sales_quantity_insights(df_products_validated, df_sales_validated)

# 2.3. Enrich Data - Combine the sales, products and stores datasets into a single enriched dataframe
df_enriched_data = enriched_data(df_products_validated, df_stores_validated, df_sales_validated, add_price_category = False)

# 2.4. PySpark UDF - PySpark UDF to categorize products based on specified price ranges
df_enriched_data_with_price_category = enriched_data(df_products_validated, df_stores_validated, df_sales_validated, add_price_category = True)

# Task: Part 3 - Data Export

# 3.1 Save the enriched dataset from Part 2, Task 3, in Parquet format, partitioned by category and transaction_date
export_dataframe_as_parquet_with_partitions(
    df_enriched_data, 
    output_path = "data/output/enriched_data.parquet", 
    partitions_list = ["category","transaction_date"]
)

# 3.2 Save the store revenue insights (from Part 2, Task 1) in CSV format.
export_dataframe_as_csv(
    df_aggregated_sales, 
    output_path = "data/output/sales_total_revenue.csv"
)
