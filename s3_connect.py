import boto3, os
from  dotenv import load_dotenv

# AWS S3 credentials
"""
Is neccessary to create a .env file in the same directory as this file
where you store your access and secret_access keys in the format of 

export aws_access_key_id = "your_access_key"
export aws_secret_access_key = "your_secret_access_key" 

"""

def s3_connection():
    
    # Loading sessetive data
    load_dotenv("/Users/miguel/Desktop/Coding/Sparta/SpartaGlobalETLPipeline/final_project/.env")
    
    aws_access_key_id = os.environ["aws_access_key_id"]
    aws_secret_access_key = os.environ["aws_secret_access_key"]
    
    # Creating an S3 client object
    s3_client = boto3.client("s3", aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    return s3_client


def get_all_keys():
    
    object_keys = []

    # Parameters for pagination
    params = {'Bucket': 'data-eng-228-final-project'}

    # Loop through the objects in the S3 bucket
    while True:
        response = s3_connection().list_objects_v2(**params)
        
        # Retrieve the object keys from the response
        for obj in response['Contents']:
            object_keys.append(obj['Key'])

        # Check if there are more objects to retrieve
        if 'NextContinuationToken' in response:
            params['ContinuationToken'] = response['NextContinuationToken']
        else:
            break
    return object_keys
    

def select_file_type(type):
    file_list = []
    for file in get_all_keys():
        if file.endswith(type):
            file_list.append(file)
    return file_list

def get_file(file_name):
    s3_file_path = f"s3://{'data-eng-228-final-project'}/{file_name}"
    return s3_file_path

    