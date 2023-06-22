import boto3
import pandas as pd
import json

s3_client = boto3.client('s3')

obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key='Talent/10383.json')
data = json.load(obj['Body'])


# # Read the file from S3
# response = s3_client.get_object(Bucket='data-eng-228-final-project', Key="Talent/Sparta Day 1 August 2019.txt")
# content = response['Body'].read().decode('utf-8')

#Display the file contents
# print(content)
# print(data)


# def show(key:str):
#     obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key=key)
#     filetype = key[key.index('.'):]
#     if filetype == '.csv':
#         data = pd.read_csv(obj['Body'])
#         return data
#     elif filetype == '.json':
#         data = json.load(obj['Body'])
#         print(data)
#     elif filetype == '.txt':
#         print(obj['Body'].read().decode('utf-8'))

# df = show("Talent/Sept2019Applicants.csv")

# for column in df.columns:
#     print(column)


# Load JSON data into a DataFrame with normalization
# data = pd.read_json('10383.json')

with open('10383.json') as f:
    data = json.load(f)

norm_data = pd.json_normalize(data)

# Display the DataFrame
print(norm_data)
