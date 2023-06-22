import boto3
import paramiko
from  dotenv import load_dotenv
import os

# AWS S3 credentials
load_dotenv()
aws_access_key_id = os.environ["aws_access_key_id"]
aws_secret_access_key = os.environ["aws_secret_access_key"]


# SSH connection details
hadoop_host = os.environ["hadoop_host"]
hadoop_username = os.environ["hadoop_username"]
hadoop_private_key = "/Users/miguel/.ssh/DataStudents.pem"

# S3 bucket and file details
s3_bucket_name = "data-eng-228-final-project"
s3_file_key = "Academy/Business_20_2019-02-11.csv"
local_file_path = "/Users/miguel/Desktop/Coding/Sparta/Python/SpartaProjects/final_project/new_file.csv"
hadoop_file_path = "~/test/test.csv"

# Establish SSH connection
ssh_client = paramiko.SSHClient()
ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
private_key = paramiko.RSAKey.from_private_key_file(hadoop_private_key)
ssh_client.connect(hadoop_host, username=hadoop_username, pkey=private_key)

# Download file from S3
s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
s3_client.download_file(s3_bucket_name, s3_file_key, local_file_path)

# Transfer file to Hadoop using SCP
sftp_client = ssh_client.open_sftp()
sftp_client.put(local_file_path, hadoop_file_path)
sftp_client.close()

# Close SSH connection
ssh_client.close()

print("Data transfer completed successfully.")
