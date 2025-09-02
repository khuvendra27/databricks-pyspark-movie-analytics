# Databricks PySpark Movie Analytics

This project implements an ETL pipeline in **Databricks Free Edition** using **PySpark** and **Delta Lake**.

## Dataset
MovieLens 25M dataset:
- movies.csv
- ratings.csv
- tags.csv

## Pipeline
- **Bronze**: Raw ingestion from CSV
- **Silver**: Cleaned and transformed data
- **Gold**: Aggregated analytics (top movies, popular genres)

## Technologies
- Databricks Free Edition
- PySpark
- Delta Lake
- GitHub

## Example Queries
- Top 10 movies by average rating
- Most popular genres by rating count
