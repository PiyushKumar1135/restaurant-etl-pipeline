# Restaurant Data ETL Pipeline

End-to-end ETL pipeline built using PySpark on Databricks. Processes a restaurant 
dataset through cleaning, type casting, and aggregation, then loads the transformed 
data into queryable SQL tables for analysis on city-wise ratings, costs, and cuisine trends.

## Tech Stack
- PySpark
- Databricks (Unity Catalog Volumes)
- SQL
- Python

## Pipeline Steps
1. **Extract:** Load raw CSV data from Unity Catalog Volume into a Spark DataFrame
2. **Transform:** Clean nulls, cast string columns to numeric types, aggregate by city and cuisine
3. **Load:** Save transformed data as queryable SQL tables for downstream analysis
