# Databricks notebook source
# MAGIC %md
# MAGIC ###About Student Dataset
# MAGIC As part of a data-driven initiative aimed at understanding key trends within student classification and performance, we are looking to explore the following questions related to the data on student academic performance and various influencing factors:
# MAGIC
# MAGIC - Does receiving a scholarship impact the number of weekly study hours?
# MAGIC - What is the relationship between study time and final grade?
# MAGIC - How does project work affect grades?
# MAGIC - To what extent do sports activities influence a student's academic success?
# MAGIC - What is the impact of a student's age on academic performance?
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing a helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using shift + enter. Note that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "data_brigade_bronze"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Ingestion
# MAGIC - **Extracting Data**: Collecting raw data from various sources such as databases, APIs, files, or streaming services.    
# MAGIC - **Loading into Storage**: Storing processed data in data lakes, warehouses, or databases for further analysis.  

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read data

# COMMAND ----------

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("StudentClassification").getOrCreate()
filepath = "dbfs:/FileStore/tables/student.csv"  
raw_df = spark.read.format("csv").option("header", True).load(filepath)

display(raw_df)

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

filepath = "dbfs:/FileStore/tables/student.csv"

def create_dataframe(filepath: str) -> DataFrame:
    student_schema = StructType([
        StructField("_c0", StringType(), True),
        StructField("Id", IntegerType(), True),
        StructField("Student_Age", IntegerType(), True),
        StructField("Gender", StringType(), True),
        StructField("High_School_Type", StringType(), True),
        StructField("Scholarship", StringType(), True),
        StructField("Additional_Work", StringType(), True),
        StructField("Sports_activity", StringType(), True),
        StructField("Transportation", StringType(), True),
        StructField("Weekly_Study_Hours", DoubleType(), True),
        StructField("Attendance", StringType(), True),
        StructField("Reading", StringType(), True),
        StructField("Notes", StringType(), True),
        StructField("Listening_in_Class", StringType(), True),
        StructField("Project_work", StringType(), True),
        StructField("Grade", StringType(), True)
    ])
    df = spark.read.format("csv") \
            .option("header", True) \
            .option("delimiter", ",") \
            .option("escape", "\\") \
            .schema(student_schema) \
            .load(filepath)
    return df

bronze_df = create_dataframe(filepath)
display(bronze_df)

# COMMAND ----------

# MAGIC %md 
# MAGIC ####Unit Test
# MAGIC In the **Bronze Layer**, we write unit tests for the following reasons:  
# MAGIC
# MAGIC - **Data Integrity Checks**: Ensures raw data is correctly ingested without corruption, missing records, or unexpected changes in format.  
# MAGIC - **Schema Validation**: Verifies that the ingested data adheres to the expected schema (column presence, data types, and structure).  
# MAGIC - **Duplicate & Anomaly Detection**: Identifies duplicate records, incorrect timestamps, or unexpected null values in raw data.

# COMMAND ----------

def test_create_dataframe():
    result = create_dataframe(filepath)

    assert result is not None
    expected_columns = [
        "_c0","Id", "Student_Age", "Gender", "High_School_Type", "Scholarship", 
        "Additional_Work", "Sports_activity", "Transportation", "Weekly_Study_Hours", 
        "Attendance", "Reading", "Notes", "Listening_in_Class", "Project_work", "Grade"
    ]
    assert sorted(result.columns) == sorted(expected_columns), f"Expected columns: {expected_columns}, but got: {result.columns}"
    
    assert result.count() == 145, f"Expected 145 rows, but got {result.count()}"

    print("All tests pass! :)")

test_create_dataframe()


# COMMAND ----------

bronze_df.printSchema()

# COMMAND ----------

# MAGIC %md 
# MAGIC ##Convert to Parquet File
# MAGIC
# MAGIC - **Optimized Storage & Compression**: Parquet files store data in a highly efficient columnar format, reducing storage space and improving read performance compared to formats like CSV or JSON.  
# MAGIC
# MAGIC - **Faster Query Performance**: Since Parquet is columnar, it allows for better I/O performance and enables Spark to read only the necessary columns, speeding up analytics and transformations.

# COMMAND ----------

from pyspark.sql import DataFrame

parquet_output_path = "dbfs:/mnt/bronze/student_data"

def write_to_parquet(input_df: DataFrame, output_path: str):
    input_df.write.mode("overwrite").parquet(output_path)
    print(f"Data successfully written to Parquet at: {output_path}")

write_to_parquet(bronze_df, parquet_output_path)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/bronze/student_data/")

# COMMAND ----------

helpers.clean_working_directory()