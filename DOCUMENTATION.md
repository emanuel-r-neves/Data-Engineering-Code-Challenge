# Approach

This project processes and analyzes retail sales data using PySpark, transforming raw data into meaningful insights. 
The goal of this app is through raw data on stores, products and sales to produce two output files with insights -- one in Parquet format and another as a CSV file.

**The steps include**:

### Part 1: Data import, preparation and validation.
- Loading raw CSV files for products, stores, and sales into Spark DataFrames.
- Ensuring that the data follows expected formats, handling null values, and removing duplicates.

### Part 2: Data transformation and aggregation.
- Aggregating sales data, calculating monthly sales insights
- Obtaining enriching sales records with product and store details.
- Providing the option of categorizing product prices based on a rule.

### Part 3: Data export and output.
- Exporting monthly sales insights as a CSV file. 
- Exporting enriched data as a Parquet file.

---

# Assumptions & Decisions

- **Data Source**: CSV files are used as raw input data. These files were placed in the following folder ```/data/input/```
- **Schema Validation**: The expected data schema is defined beforehand for products, stores, and sales and is enforced in runtime.
- **Data Quality**: The code handles missing values in the sales data (quantity, price, transaction_date) and removes duplicates for all raw data based on id (e.g. ```transaction_id```, ```store_id```, ```product_id```).
- **Price Categorization** -- This treatment was designed as **optional** when obtaining enriched data and the price ranges are defined as follows:
    - **Low**: Below 20
    - **Medium**: Between 20 and 100
    - **High**: Above 100
- **Data Export**: the output files were set to be placed in the following folder ```/data/output/```
---

# Steps to Run the Code

### On your local machine:
1. Clone the repository;
2. Install dependencies mentioned in the requirements.txt file on your local machine;
3. Then run on the main script: ```python main.py```
4. For running the tests: ```python -m pytest tests.py```
5. Once the script runs successfully, the output files will be available at the directory ```/data/outputs/```

---

# Expected Outputs

- **Parquet file**: Enriched Sales Data -- The file should combine sales info with products and stores details into a single enriched dataframe. Additionally, the Parquet file should be partitioned by product category and transaction date.
- **CSV file**: Sales Aggregation -- The file should include the total monthly revenue for each store/product category. 
