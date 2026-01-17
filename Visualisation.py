# Databricks notebook source
# MAGIC %md
# MAGIC ###Visualisations
# MAGIC In this notebook, we'll take our data from the Gold layer and create a visualisation. We want to show a distribution of charge dispensed along with the mean, median and range.
# MAGIC
# MAGIC There are many tools that we can use to generate Visualisations. In this exercise we'll explore visualisation creation using:
# MAGIC
# MAGIC Databricks Graphs
# MAGIC Plotly
# MAGIC
# MAGIC Note-To run this succesfully, we have to import earlier layer as module

# COMMAND ----------

# MAGIC %md
# MAGIC ###Set up this Notebook
# MAGIC Before we get started, we need to quickly set up this notebook by installing helpers, cleaning up your unique working directory (as to not clash with others working in the same space), and setting some variables. Run the following cells using Shift + Enter. NOTE: that if your cluster shuts down, you will need to re-run the cells in this section.

# COMMAND ----------

# MAGIC %pip uninstall -y databricks_helpers
# MAGIC %pip install git+https://github.com/data-derp/databricks_helpers#egg=databricks_helpers

# COMMAND ----------

exercise_name = "visualisation"

# COMMAND ----------

filepath = f"dbfs:/mnt/gold/student_data/"

# COMMAND ----------

from databricks_helpers.databricks_helpers import DataDerpDatabricksHelpers

helpers = DataDerpDatabricksHelpers(dbutils, exercise_name)

current_user = helpers.current_user()
working_directory = helpers.working_directory()

print(f"Your current working directory is: {working_directory}")

# COMMAND ----------

## This function CLEARS your current working directory. Only run this if you want a fresh start or if it is the first time you're doing this exercise.
helpers.clean_working_directory()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Read Data from Gold Layer
# MAGIC Let's read the parquet files that we created in the Gold layer!

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
# MAGIC ###Question 1

# COMMAND ----------

import 
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
# MAGIC ###Question 2
# MAGIC

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
# MAGIC ###Question 3
# MAGIC

# COMMAND ----------

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
# MAGIC ###Question 4

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

df_pd = sql_result.toPandas()

sns.barplot(data=df_pd, x="Sports_activity", y="high_performance_rate")

plt.show()