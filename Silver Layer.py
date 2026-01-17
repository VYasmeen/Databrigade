# Databricks notebook source
# MAGIC %md
# MAGIC ##Silver Layer
# MAGIC The Silver Layer is where we clean, validate, and transform raw data into a more structured and usable format.
# MAGIC - Removes raw data inconsistencies from the Bronze layer.
# MAGIC - Creates a structured, cleaned, and validated dataset.
# MAGIC - Prepares data for the Gold layer, where we apply analytics & reporting.
# MAGIC

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

exercise_name = "data_brigade_silver"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read from Bronze layer data

# COMMAND ----------

from pyspark.sql import DataFrame

bronze_input_path = "dbfs:/mnt/bronze/student_data/"

def read_from_bronze(bronze_path: str) -> DataFrame:
    return spark.read.format("parquet").load(bronze_path)

silver_df = read_from_bronze(bronze_input_path)

display(silver_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Unit Test

# COMMAND ----------

def test_read_from_bronze_unit():
    df = read_from_bronze(bronze_input_path)
    assert df is not None, "DataFrame is None, expected non-empty DataFrame"
    print("Test_read_from_bronze PASSED!")

test_read_from_bronze_unit()

# COMMAND ----------

# MAGIC %md
# MAGIC ####E2E Test

# COMMAND ----------

def test_e2e():
    df = read_from_bronze(bronze_input_path)
    assert df.count() > 0, f"E2E Failure: Expected rows > 0, got {df.count()}"
test_e2e()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Check null values
# MAGIC If output is all zeroes it mean is has no null values! Let us check it! Here we go ...
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, sum

silver_df.select([sum(col(c).isNull().cast("int")).alias(c) for c in silver_df.columns]).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ####Clean Data
# MAGIC 1. Drop unnessary columns
# MAGIC 2. Cast types as per requirement
# MAGIC 3. Remove null values if they are present else no need
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when

# Create a new column 'Grade_Numeric' for the numeric value of grades
silver_df = silver_df.withColumn(
    "Grade_Numeric",
    when(col("Grade") == "AA", 8)
    .when(col("Grade") == "BA", 7)
    .when(col("Grade") == "BB", 6)
    .when(col("Grade") == "CB", 5)
    .when(col("Grade") == "CC", 4)
    .when(col("Grade") == "DB", 3)
    .when(col("Grade") == "DC", 2)
    .when(col("Grade") == "DD", 1)
    .when(col("Grade") == "Fail", 0)
    .otherwise(None)
)

# silver_df = silver_df.withColumn(
#     "Gender",
#     when(col("Gender") == "Male", 1)
#     .when(col("Gender") == "Female", 0)
#     .otherwise(None)
# )

# silver_df = silver_df.withColumn(
#     "Additional_Work",
#     when(col("Additional_Work") == "Yes", 1)
#     .when(col("Additional_Work") == "No", 0)
#     .otherwise(None)
# )

# silver_df = silver_df.withColumn(
#     "Reading",
#     when(col("Reading") == "Yes", 1)
#     .when(col("Reading") == "No", 0)
#     .otherwise(None)
# )

# silver_df = silver_df.withColumn(
#     "Notes",
#     when(col("Notes") == "Yes", 1)
#     .when(col("Notes") == "No", 0)
#     .otherwise(None)
# )

# silver_df = silver_df.withColumn(
#     "Listening_in_Class",
#     when(col("Listening_in_Class") == "Yes", 1)
#     .when(col("Listening_in_Class") == "No", 0)
#     .otherwise(None)
# )

# silver_df = silver_df.withColumn(
#     "Attendance",
#     when(col("Attendance") == "Never", 0)
#     .when(col("Attendance") == "Sometimes", 1)
#     .when(col("Attendance") == "Always", 2)
#     .otherwise(None)
# )
# silver_df = silver_df.withColumn(
#     "Project_work",
#     when(col("Project_work") == "Yes", 1)
#     .when(col("Project_work") == "No", 0)
#     .otherwise(None)
#)

display(silver_df)

# COMMAND ----------

columns_to_drop = ["_c0", "Transportation", "Reading", "Notes", "High_School_Type", "Gender"]

silver_df = silver_df.drop(*columns_to_drop)
display(silver_df)





# COMMAND ----------

before_count = silver_df.count()

silver_df=silver_df = silver_df.filter(~(col("Id").isin(5113, 5127)))

after_count = silver_df.count()

print(f"Before Filtering: {before_count} rows")
print(f"After Filtering: {after_count} rows")
display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ####Unit Test

# COMMAND ----------

def test_column_transformations_unit():
    assert "Grade_Numeric" in silver_df.columns, "Grade_Numeric column missing"
    assert "Attendance" in silver_df.columns, "Attendance column missing"
    print("Transformations PASSED!")

test_column_transformations_unit()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Type casting

# COMMAND ----------

from pyspark.sql.functions import col, regexp_replace, when
from pyspark.sql.types import DoubleType


silver_df = silver_df.withColumn(
    "Scholarship",
    when(col("Scholarship") == "None", 0.0)  
    .otherwise(regexp_replace(col("Scholarship"), "%", "").cast(DoubleType()))/100.0
)

display(silver_df)


# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit Test

# COMMAND ----------

def test_typecasting_unit():
    assert "Scholarship" in silver_df.columns, "Scholarship column missing"
    print("Typecasting PASSED!")

test_typecasting_unit()

# COMMAND ----------

silver_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ##Write to Parquet
# MAGIC

# COMMAND ----------

from pyspark.sql import DataFrame
parquet_output_path = "dbfs:/mnt/silver/student_data"

def write_to_parquet(input_df: DataFrame, output_path: str):
    input_df.write.mode("overwrite").parquet(output_path)
    print(f"Data successfully written to Parquet at: {output_path}")

write_to_parquet(silver_df, parquet_output_path)


# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/silver/student_data")

# COMMAND ----------

helpers.clean_working_directory()