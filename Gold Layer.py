# Databricks notebook source
# MAGIC %md
# MAGIC ###Gold Layer
# MAGIC
# MAGIC - **Aggregated & Curated Data** – The data is transformed into meaningful insights, such as KPIs, trends, and summaries for decision-making.  
# MAGIC - **Optimized for Performance** – The data is structured for fast querying, reducing computation time for reports and dashboards.  
# MAGIC - **Used for Business Intelligence (BI) & Machine Learning (ML)** – The refined data is ready for predictive analytics, visualization, and business applications.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers exercise_ev_databricks_unit_tests
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers git+https://github.com/data-derp/exercise_ev_databricks_unit_tests#egg=exercise_ev_databricks_unit_tests

# COMMAND ----------

from pyspark.sql.functions import when, sum, abs, first, last, lag, col, count
from pyspark.sql.window import Window
from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

# COMMAND ----------

exercise_name = "data_brigade_gold"

# COMMAND ----------

filepath = f"dbfs:/mnt/silver/student_data/"

# COMMAND ----------

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Read data from silver layer

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

def read_parquet(filepath: str) -> DataFrame:
    df = spark.read.parquet(filepath)
    return df

# COMMAND ----------

gold_df = read_parquet(filepath)

display(gold_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #####Questions
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #####1-Does receiving a scholarship impact the number of weekly study hours?

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg

def get_average_study_hours_by_scholarship(input_df: DataFrame):
    df_filtered = input_df.filter(
        (col("Scholarship").isin([0.25, 0.5, 0.75, 1.0])) 
    )
    
    return df_filtered.groupBy("Scholarship") \
                      .agg(avg("Weekly_Study_Hours").alias("average_study_hours")) \
                      .orderBy("Scholarship")

gold_df.transform(get_average_study_hours_by_scholarship).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit test
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg
import pytest

spark = SparkSession.builder.master("local").appName("Test").getOrCreate()

def get_avg_study_hours(df):
    return df.filter(col("Scholarship").isin([0.25, 0.5, 0.75, 1.0])) \
             .groupBy("Scholarship") \
             .agg(avg("Weekly_Study_Hours").alias("avg_study_hours")) \
             .orderBy("Scholarship")

def test_avg_study_hours():
    data = [(0.25, 10), (0.25, 20), (0.5, 15), (0.5, 25), (0.75, 30), (1.0, 10), (1.0, 15), (0.0, 5)]
    df = spark.createDataFrame(data, ["Scholarship", "Weekly_Study_Hours"])
    expected = [(0.25, 15.0), (0.5, 20.0), (0.75, 30.0), (1.0, 12.5)]
    assert get_avg_study_hours(df).collect() == spark.createDataFrame(expected, ["Scholarship", "avg_study_hours"]).collect()

test_avg_study_hours()
print("Unit Test Passed!")


# COMMAND ----------

# MAGIC %md
# MAGIC ###E2E test
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, avg

def get_avg_study_hours(df):
    return df.filter(col("Scholarship") != 0.0) \
             .groupBy("Scholarship") \
             .agg(avg("Weekly_Study_Hours").alias("avg_study_hours"))

def test_e2e_avg_study_hours():
    data = [(0.25, 5), (0.5, 10), (0.75, 25), (1.0, 5), (1.0, None), (0.0, 8)]
    df = spark.createDataFrame(data, ["Scholarship", "Weekly_Study_Hours"])
    result = get_avg_study_hours(df)

    assert result.count() == 4  # Only 0.25, 0.5, 0.75, 1.0 should be present
    assert result.filter(col("Scholarship") == 0.0).count() == 0  # Ensure 0.0 is excluded
    assert result.filter(col("avg_study_hours").isNull()).count() == 0  # No null averages

test_e2e_avg_study_hours()
print("E2E Test Passed!")

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

df = gold_df.transform(get_average_study_hours_by_scholarship).toPandas()

sns.barplot(x="Scholarship", y="average_study_hours", data=df, palette="Blues_r")
plt.title("Avg Study Hours by Scholarship", fontsize=14, fontweight="bold")

for p in plt.gca().patches:
    plt.text(p.get_x() + p.get_width()/2, p.get_height(), f"{p.get_height():.1f}", 
             ha="center", va="bottom", fontsize=10)

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ####Unit test

# COMMAND ----------

# MAGIC %md
# MAGIC #####2-What is the relationship between study time and final grade?

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, avg, corr

def study_time_vs_grade(input_df: DataFrame):
    df_filtered = input_df.filter(
        (col("Weekly_Study_Hours") > 0) & 
        (col("Grade_Numeric").isNotNull())
    )
    
    avg_study_hours_per_grade = df_filtered.groupBy("Grade_Numeric") \
                                           .agg(avg("Weekly_Study_Hours").alias("avg_study_hours")) \
                                           .orderBy("Grade_Numeric")

    correlation = df_filtered.select(corr("Weekly_Study_Hours", "Grade_Numeric").alias("correlation")).collect()[0][0]

    print(f"Correlation between Weekly Study Hours and Grade: {correlation}")
    
    return avg_study_hours_per_grade

gold_df.transform(study_time_vs_grade).display()


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

pandas_df = gold_df.transform(study_time_vs_grade).toPandas()

sns.set_theme(style="whitegrid")

plt.figure(figsize=(8, 5))
ax = sns.lineplot(
    x=pandas_df["Grade_Numeric"], 
    y=pandas_df["avg_study_hours"], 
    marker="o", 
    color="b",
    linewidth=2
)

plt.xlabel("Grade Numeric", fontsize=12)
plt.ylabel("Average Weekly Study Hours", fontsize=12)
plt.title("Study Time vs Grade", fontsize=14, fontweight='bold')

for x, y in zip(pandas_df["Grade_Numeric"], pandas_df["avg_study_hours"]):
    plt.text(x, y, f"{y:.1f}", ha="right", va="bottom", fontsize=10)

plt.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ##Unit Test

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest

spark = SparkSession.builder.master("local").appName("test").getOrCreate()


test_data = [
    (5, 7), (10, 6), (0, 5), (15, 7), (8, 6), (12, None)
]
columns = ["Weekly_Study_Hours", "Grade_Numeric"]
test_df = spark.createDataFrame(test_data, columns)

def test_study_time_vs_grade():
    result_df = study_time_vs_grade(test_df)

    
    assert set(result_df.columns) == {"Grade_Numeric", "avg_study_hours"}
    assert result_df.filter(col("Grade_Numeric").isNull()).count() == 0
    assert result_df.count() > 0

test_study_time_vs_grade()
print("All tests passed!")


# COMMAND ----------

# MAGIC %md
# MAGIC ###E2E Test

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pytest


spark = SparkSession.builder.master("local").appName("e2e_test").getOrCreate()


test_data = [
    (5, 7), (10, 6), (0, 5), (15, 7), (8, 6), (12, None), 
    (0, None), (20, 8), (0, 3), (7, 4)  
]
columns = ["Weekly_Study_Hours", "Grade_Numeric"]
test_df = spark.createDataFrame(test_data, columns)

def test_study_time_vs_grade_e2e():
    # Step 1: Apply transformation
    result_df = study_time_vs_grade(test_df)

    # Step 2: Validate schema
    assert set(result_df.columns) == {"Grade_Numeric", "avg_study_hours"}

    # Step 3: Ensure no null values in Grade_Numeric
    assert result_df.filter(col("Grade_Numeric").isNull()).count() == 0

    # Step 4: Ensure the output is sorted correctly
    sorted_df = result_df.orderBy("Grade_Numeric")
    assert result_df.collect() == sorted_df.collect()

    # Step 5: Ensure valid correlation calculation (Shouldn't be None)
    correlation_value = test_df.selectExpr("corr(Weekly_Study_Hours, Grade_Numeric)").collect()[0][0]
    assert correlation_value is not None

test_study_time_vs_grade_e2e()
print("E2E test passed successfully!")


# COMMAND ----------

# MAGIC %md
# MAGIC #####3-How does project work affects grades?

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import avg, col

def get_avg_grade_by_factors(input_df: DataFrame):
    df_grouped = input_df.groupBy("Project_work") \
                         .agg(avg(col("Grade_Numeric")).alias("avg_grade")) \
                         .orderBy("Project_work")

    df_grouped.show()
    df_grouped.createOrReplaceTempView("student_data")

    sql_result = spark.sql("""
        SELECT Project_work, avg_grade
        FROM student_data
        ORDER BY Project_work
    """)

    return df_grouped, sql_result

pyspark_result, sql_result = get_avg_grade_by_factors(gold_df)

pyspark_result.show()  


# COMMAND ----------

# MAGIC %md
# MAGIC ####Unit Test

# COMMAND ----------

def get_avg_grade_by_factors(df):
    return df.groupBy("Project_work") \
             .agg(avg("Grade_Numeric").alias("avg_grade")) \
             .orderBy("Project_work")

def test_avg_grade_by_factors():
    data = [("Yes", 8), ("Yes", 7), ("No", 6), ("No", 5)]
    df = spark.createDataFrame(data, ["Project_work", "Grade_Numeric"])
    
    expected = [("No", 5.5), ("Yes", 7.5)]
    expected_df = spark.createDataFrame(expected, ["Project_work", "avg_grade"])
    
    assert get_avg_grade_by_factors(df).collect() == expected_df.collect()

test_avg_grade_by_factors()
print("Unit Test Passed!")

# COMMAND ----------

#visualisation
import matplotlib.pyplot as plt
import seaborn as sns

pandas_df = pyspark_result.toPandas()


plt.figure(figsize=(8, 5))
sns.barplot(x="Project_work", y="avg_grade", data=pandas_df, palette="Blues")


plt.xlabel("Project Work Involvement")
plt.ylabel("Average Grade")
plt.title("Impact of Project Work on Grades")

plt.show()



# COMMAND ----------

# MAGIC %md
# MAGIC #####4-To what extent do sports activities influence a student's academic success?

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when, count, sum

def analyze_sports_impact(input_df: DataFrame):
    df_transformed = input_df.withColumn(
        "Sports_activity_numeric",
        when(col("Sports_activity") == "Yes", 1).when(col("Sports_activity") == "No", 0)
    )
    
    df_transformed.createOrReplaceTempView("student_sports_data")
    sql_result = spark.sql("""
        SELECT 
            Sports_activity, 
            COUNT(*) AS student_count, 
            SUM(CASE WHEN Grade_Numeric >= 5 THEN 1 ELSE 0 END) AS high_performers, 
            ROUND((SUM(CASE WHEN Grade_Numeric >= 5 THEN 1 ELSE 0 END) * 100.0) / COUNT(*), 2) AS high_performance_rate
        FROM student_sports_data
        GROUP BY Sports_activity
    """)

    return df_transformed, sql_result

pyspark_df, sql_result = analyze_sports_impact(gold_df)

sql_result.display()


# COMMAND ----------

# MAGIC %md
# MAGIC ###Unit Test

# COMMAND ----------

#unit test
from pyspark.sql import Row

def test_analyze_sports_impact_no_effect():
    test_data = [
        Row(Sports_activity="Yes", Grade_Numeric=6),
        Row(Sports_activity="Yes", Grade_Numeric=5),
        Row(Sports_activity="No", Grade_Numeric=6),
        Row(Sports_activity="No", Grade_Numeric=5),
    ]
    test_df = spark.createDataFrame(test_data)
    _, result_df = analyze_sports_impact(test_df)

    expected_data = [
        Row(Sports_activity="Yes", student_count=2, high_performers=2, high_performance_rate=100.0),
        Row(Sports_activity="No", student_count=2, high_performers=2, high_performance_rate=100.0),
    ]
    expected_df = spark.createDataFrame(expected_data)

    assert result_df.collect() == expected_df.collect()
    print("Test Passed: Sports activity does not affect grades")

test_analyze_sports_impact_no_effect()


# COMMAND ----------

#visualisation code
import matplotlib.pyplot as plt
import seaborn as sns

df_pd = sql_result.toPandas()

sns.barplot(data=df_pd, x="Sports_activity", y="high_performance_rate")

plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC #####5-What is the impact of a student's age on academic performance?

# COMMAND ----------

from pyspark.sql import DataFrame
from pyspark.sql.functions import when, avg, round, col

def analyze_age_impact(input_df: DataFrame):
    return (input_df.withColumn("Student_Age_Group",
                when(col("Student_Age") < 18, "Below 18")
                .when((col("Student_Age") >= 18) & (col("Student_Age") <= 22), "18-22")
                .otherwise("Above 22"))
            .groupBy("Student_Age_Group")
            .agg(round(avg("Grade_Numeric"), 2).alias("avg_grade"))
            .orderBy("Student_Age_Group"))

result_df = analyze_age_impact(gold_df)
result_df.display()


# COMMAND ----------

# MAGIC %md
# MAGIC #####1-

# COMMAND ----------

gold_df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Write to Parquet

# COMMAND ----------

from pyspark.sql import DataFrame
parquet_output_path = "dbfs:/mnt/gold/student_data"

def write_to_parquet(input_df: DataFrame, output_path: str):
    input_df.write.mode("overwrite").parquet(output_path)
    print(f"Data successfully written to Parquet at: {output_path}")

write_to_parquet(gold_df, parquet_output_path)


# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/gold/student_data")

# COMMAND ----------

helpers.clean_working_directory()