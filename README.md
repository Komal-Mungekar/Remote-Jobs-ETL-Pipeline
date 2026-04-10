# Remote Jobs ETL Pipeline

**An automated data pipeline that scrapes remote job listings daily and stores them in a PostgreSQL database built with Apache Airflow and Docker.**


## What this project does

This project builds a fully automated ETL (Extract, Transform, Load) pipeline using Apache Airflow. Every day, it automatically:

1. Extracts remote job listings from [RemoteOK](https://remoteok.com) (a popular remote job board with a public API)
2. Transforms the raw data, it cleans whitespace, removes duplicates, handles missing fields
3. Loads the cleaned data into a PostgreSQL database

You can then query the database to analyse trends in remote work - popular job titles, common skills (tags), salary ranges and more.



