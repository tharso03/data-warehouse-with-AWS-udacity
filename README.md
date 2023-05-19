# Project: Data Warehouse

This is a project for the AWS nanodegree on data engineering.

## Project description

In this project, we are considergin a hypothetical scenario: a music streaming startup, Sparkify, wants to move their data from an S3 bucket, where it is stored as JSON files, to AWS Redshift.

There are three datasets in their S3 bucket: 
* `song_data`: contains data on the songs in the Sparkify app, as well as the artists who composed them.
* `log_data`: contains data about user activity in the app, also called logs.
* `log_json_path`: contains metadata that is required by AWS to correctly load `log_data`.

## Goals

Using Python, we will create PostgreSQL tables with the data from the `song_data`and `log_data` datasets. These are called the **staging tables**.

The staging tables are:
* `staging_events`: contains data from `log_data`.
* `staging_logs`: contains data from `song_data`.

Then, we transform this data to select only logs that refer to song plays and create two new types of tables: **fact tables**, which contain data on which songs users are listening - which is the data being analyzed, and **dimension tables**, which contain data that give context to the data in the fact tables.

The fact and dimension tables are:
* Fact tables: `songplays`.
* Dimension tables: `users`, `songs`, `artists`, `time`.

Finally, we load the data into AWS Redshift.

Here's a diagram of the tables:

![Sparkify diagram](/sparkify-diagram.png)

## How each file works

There are five files in this repository:
* `sql_queries.py`: a python script with SQL queries to create, fill, and delete tables.
* `create_tables.py`: a python script that creates the fact and dimention tables.
* `etl.py`: a python script to get the data from the S3 buckets and load them into fact and dimention tables.
* `dwg.cfg`: a configuration file that contains the necessary information to connect to the Redshift cluster and IAM role.
* `README.md` : this file you are reading right now. 

