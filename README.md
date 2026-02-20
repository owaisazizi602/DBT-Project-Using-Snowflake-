# DBT-Project-Using-Snowflake-


---------------------------------   >  Hotel Bookings Data Pipeline Project <---------------------------------  

This project implements a full medallion architecture for hotel booking data using modern cloud data engineering tools.

Data Ingestion: Raw hotel booking data is ingested from the source and stored in AWS S3 as the landing layer.

Bronze Layer: Using Snowpipe in Snowflake, the data is loaded into the bronze layer. dbt is used to orchestrate the ingestion while handling late-arriving data to ensure accuracy and consistency.

Silver Layer: Transformations and data cleaning are applied using dbt, standardizing fields and preparing the data for analytics.

Gold Layer (OBT): A One Big Table (OBT) is created combining silver tables, along with additional fact and dimension tables for analytics-ready consumption.

Version Control & Automation: The project is maintained with Git and GitHub, with GitHub Actions automating dbt runs and deployments.

Tech Stack: Snowflake, dbt, AWS S3, Git, GitHub, GitHub Actions

This pipeline demonstrates end-to-end ETL/ELT, incremental modeling, handling of late-arriving data, and creation of analytics-ready gold layer tables in a cloud-native environment.






<img width="1536" height="1024" alt="ada21b52-d346-43d8-94f7-685f712a9966" src="https://github.com/user-attachments/assets/84c341ca-2ca7-4928-af76-ddd9eb47d931" />

