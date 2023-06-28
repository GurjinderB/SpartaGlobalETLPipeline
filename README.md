
# Sparta Global ETL Pipeline

This repository contains an example ETL (Extract, Transform, Load) pipeline implemented in Python using Boto3 and Pandas libraries. The pipeline demonstrates how to retrieve data from an S3 bucket, perform data transformation using Pandas, and load the transformed data back into another S3 bucket. It tracks the progress of individual Spartan in SpartaGlobal Academy.

# Prerequisites
Python 3.8 installed
Boto3 library installed (pip install boto3)
Pandas library installed (pip install pandas)

# To set up environment before running the project

To get this project up and running you should start by having Python installed on your computer. We would advise to create a virtual environment for this project. You can install virtualenv with:

``` pip3 install virtualenv ```

Clone or download this repository and open it in your editor of choice. In a terminal (mac/linux) or windows terminal, run the following command in the base directory of this project:

``` virtualenv env ```
 
That will create a new folder env in your project directory. Next activate it with this command on mac/linux:

``` source env/bin/active ```

Then install the project dependencies with

``` pip install -r requirements.txt ```


# Running the project

Update the AWS credentials in the etl_pipeline.py file:

Replace 'YOUR_AWS_ACCESS_KEY_ID' with your AWS access key ID.
Replace 'YOUR_AWS_SECRET_ACCESS_KEY' with your AWS secret access key.
Replace 'YOUR_S3_BUCKET_NAME' with the name of your S3 bucket containing the input data.
Replace 'YOUR_S3_OUTPUT_BUCKET_NAME' with the name of your S3 bucket where the transformed data will be stored.


Additional Notes
Ensure that the AWS credentials used have sufficient permissions to access the specified S3 buckets.
The script assumes that the input file is in CSV format. Modify the data loading and transformation steps accordingly if working with a different file format.

This project is licensed under the MIT License. See the LICENSE file for more information.