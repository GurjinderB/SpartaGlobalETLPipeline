import boto3
from  dotenv import load_dotenv
import os

# AWS S3 credentials
"""
Is neccessary to create a .env file in the same directory as this file
where you store your access and secret_access keys in the format of 

export aws_access_key_id = "your_access_key"
export aws_secret_access_key = "your_secret_access_key" 

"""

load_dotenv()
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]


# S3 bucket and file details
s3_bucket_name = "data-eng-228-final-project"
s3_file_key = "Academy/Business_20_2019-02-11.csv"
local_file_path = "your_directory/file_name"


# Download file from S3
s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_client.download_file(s3_bucket_name, s3_file_key, local_file_path)


print("Data transfer completed successfully.")
