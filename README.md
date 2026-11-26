# Retail Data Engineering Project

## Overview

Academic project developed as part of the Data Engineering course at HSE University in collaboration with Magnit Tech.

The project covers data modeling, SQL-based transformations, distributed data processing, data quality testing, and ETL/ELT approaches using retail data.

## Project Structure

### dbt (models/, macros/, tests/)

- SQL-based data transformations.
- Staging, intermediate, and data mart models.
- Generic and custom data quality tests.
- Reusable dbt macros.
- Working with different dbt materializations.
- Working with PostgreSQL and Greenplum.

### Apache Spark (store_hse/)

- Data extraction from PostgreSQL using PySpark.
- Data storage in MinIO using the Parquet format.
- Incremental data loading and processing.
- Partition pruning.
- Data cleaning, joins, and aggregations.
- Building analytical data marts.

The Spark pipeline follows a layered data architecture:

PostgreSQL → Raw → Staging → Intermediate → Data Marts

## Technology Stack

- **Programming:** Python, SQL
- **Databases:** PostgreSQL, Greenplum
- **Data Transformation:** dbt
- **Distributed Processing:** Apache Spark (PySpark)
- **Object Storage:** MinIO (S3-compatible)
- **Data Format:** Apache Parquet
- **Development Tools:** Docker, Git, Linux

## Future Development

- Workflow orchestration with Apache Airflow.
- Pipeline automation and monitoring.
- Further exploration of ETL and ELT approaches.
- Query and data processing performance optimization.

## Project Status

Work in progress.

Academic year: 2026–2027.

## Author

Mark Polonskiy

Bachelor's student in Economics and Data Science at HSE University.
