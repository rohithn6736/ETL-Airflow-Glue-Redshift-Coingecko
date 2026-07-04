# ETL-Airflow-Glue-Redshift-Coingecko
<img width="2932" height="1580" alt="Blank diagram (3)" src="https://github.com/user-attachments/assets/0a31fb2f-a346-4e3a-a299-feb89998c3e6" />
## Overview
This project implements an end-to-end ETL pipeline for cryptocurrency market data using the CoinGecko API. Data is ingested into Amazon S3, transformed and validated using AWS Glue and AWS Glue Data Quality, loaded into Amazon Redshift Serverless for analytics, and visualized with Amazon QuickSight. The entire workflow is orchestrated using Apache Airflow running on an Amazon EC2 instance.

