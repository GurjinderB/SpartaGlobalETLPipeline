import boto3
from  dotenv import load_dotenv
import os

# AWS S3 credentials
load_dotenv()
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]


# S3 bucket and file details
s3_bucket_name = "data-eng-228-final-project"
s3_file_key = "Academy/Business_20_2019-02-11.csv"
local_file_path = "/Users/miguel/desktop/coding/sparta/SpartaGlobalETLPipeline/new_file.csv"


# Download file from S3
s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_client.download_file(s3_bucket_name, s3_file_key, local_file_path)


print("Data transfer completed successfully.")
