# data-waddle

## Explanation about components

### download_dataset.py

    This python script downloads csv files from 3 different mock URLs and save them to a directory present in the current working directory. The mock URLs are actually github URLs that points to csv files stored in a public github repo.

### travel.sql

    This SQL script creates tables such as  customer, destination and booking into a PostgreSQL database. The table schema is designed based on the csv file structure mentioned in the given task. The tables customer and destination are fact tables
    where as the table booking is a dimension table which has many to many relation with the tables customer and destination.
    Please make sure that the database is created before running the script.

### transform_load_csv_data.py

    This script performs two subtasks.
    1. Cleanse and transform the downloaded data that includes actions like converting the date format, handling missing data, and creates
    a calculated field for total_booking_value and adds it to the dataframe that holds booking data.

    2. Loads the transformed data into  previously created database which contains the tables customer, destination and booking which also ensures handling relationship between different tables like bookings &customers and also bookings & destinations.

### batch_transfer_data_to_cloud_storage.py

    This file implements the process of batch-transferring data from the postgreSQL database to a cloud storage solution(like AWS S3). Once read, the data is compressed in a format "zip" and updated in the s3 bucket based on the directory given.

### env.example

    This is an example of the env file that should be used before running the scripts. Please create a copy of this file and rename it as .env and update the necessary values mentioned in the file.

### lambda/lambda_function.py

    This is the lambda function file that reads destination and booking data from the given s3 bucket
    based on the current date there by calculating metrics like total_bookings_per_destination,
    total_booking_value_per_destination, total_passengers_booked_per_destination.
    Here we are considering both booking and destination data to find which destination doesn't have any booking.
    While running this file in AWS lambda, please increase the memory size upto atleast upto 1024 MB
    and also create a layer to add the package pandas by clicking on the add layer button.

### lambda/env.example

    This is an example of env file that is needed to run the lambda function. The environment variables can be set from the lambda function itself.

### requirements.txt

    This file contains all the necessary packages which need to be installed for the successful execution of the all the scripts.

## Pipeline/ security structure

- ### Github repo

  Make the code base of the github repository to private before pushing data files into it and set desired access level for each person (eg: read access, write access).

- ### Postgres Database

  Ensure to use strong and complex passwords to the database accounts minimum of 32 character alphanumeric character.
  Implement role-based access control (RBAC) to assign privileges and permissions to the database users based on their roles and responsibilities.

- ### Cloud related enviroment variables

  Inorder to batch transfer data into a clou
  d storage service (eg:AWS s3), make sure the secret keys are either handled in an environment variable or utilizing aws secret manager.

- ### S3 Bucket
  The s3 bucket should have custom policies / permissions that allow only certain aws services to access its get and update methods.

### Lambda Function (Serverless Function)

- The lambda functions should have proper execution roles that has the permission to access the s3 bucket.It should also have permission to event bridge, cloud watch to periodically trigger the lambda (eg: using cronjob), and to generate the logs in the cloud watch.

- The keys used in the lambda function should either be stored as environment variables or accessed from aws secrets manager.

- The analyzed data that is generated from the lambda can be pushed to any other service.

### travel.sh

    To perform the subtasks from downloading csv files to batch transfer data to cloud storage in a single step, run this shell script.
    Please make sure to keep an empty postgres database before running it.
    Also replace with necessary values to the command that runs the sql script.
