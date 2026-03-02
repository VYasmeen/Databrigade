# Databrigade

Databrigade is a **data engineering project** that demonstrates a full data pipeline built using Python and PySpark (on Databricks).  
It illustrates how data moves through layered architecture — from raw ingestion to transformation, analytics, and visualization — using structured Parquet formats and unit/E2E tests.

## Overview

This project uses a **student performance dataset** to analyze academic trends and answer data questions related to:

- Scholarship impact on study hours  
- Relationship between study time and grades  
- Effect of project work on grades  
- Influence of sports activities  
- Impact of student age on performance

The pipeline is organized into layers that align with modern data engineering principles.

## Project Structure

### 1. **Bronze Layer**
This layer ingests raw CSV data, applies schema, and writes it to Parquet format for optimized storage and later stages.  
It includes basic **data integrity and schema validation tests**.

File: `Bronze Layer.py`

### 2. **Silver Layer**
This layer reads the Bronze Parquet data, cleans and transforms it, handles type casting, removes unnecessary attributes, and prepares structured data.  
It includes **validation and transformation tests**.

File: `Silver Layer.py`

### 3. **Gold Layer**
The Gold layer performs analysis and business-level logic on cleaned data. It includes:
- Aggregation queries
- Grouped analytics
- Correlations
- Unit and end-to-end tests  
It also includes data visualizations.

File: `Gold Layer.py`

### 4. **Visualisation**
This module builds visual charts using Matplotlib and Seaborn from the final Gold layer results to present insights visually.

File: `Visualisation.py`

## Key Features

- **Layered Data Engineering Architecture:** Bronze → Silver → Gold  
- **Use of Parquet Format:** Efficient columnar storage  
- **PySpark for Scalable Processing**
- **Unit & End-to-End Tests:** Ensures data quality and correctness  
- **Visualizations:** For analytical insights

## How to Run

1. Load the dataset (student.csv) into your Databricks FileStore or DBFS path.
2. Run each layer notebook/script in sequence:
   - `Bronze Layer.py`
   - `Silver Layer.py`
   - `Gold Layer.py`
   - `Visualisation.py`
3. Ensure Spark cluster is active and configured.

## Technologies Used

- Python
- PySpark
- Databricks
- Parquet file format  
- Matplotlib & Seaborn (visualization)

## Insights

This project aims to demonstrate how modern data engineering pipelines are built using principles like:
- Schema validation
- Clean transformations
- Aggregations and analytics
- Testing at each layer  
- Visual storytelling of data

---
