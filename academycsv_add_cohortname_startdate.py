# python script which ensures all behaviour scores are integers and adds two columns:
# cohort name, e.g: 'Business_20'
# start date, converted to datetime
# 
# output is three dataframes, each containing data
# uses Micheal A academy_extraction work

import boto3
import pandas as pd
import sqlite3
import io

db = sqlite3.connect('Sparta_DB')
cursor = db.cursor()
s3_client = boto3.client('s3')
bucket_name = 'data-eng-228-final-project'
prefix = 'Academy/'
response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
files = len(response['Contents'])

business = []
data = []
engineering = []

for obj in response['Contents']:
    key = obj['Key']
    if key.startswith('Academy/B'):
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        # Add cohort name column
        second_underscore_index = key.index('_', key.index('_') + 1)
        df['course_name'] = key[key.index('/') + 1 : second_underscore_index ]
        # Add start date column
        df['start_date'] = key[second_underscore_index + 1 : key.index('.')]
        business.append(df)

    elif key.startswith('Academy/D'):
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        # Course name, including course index
        second_underscore_index = key.index('_', key.index('_') + 1)
        df['course_name'] = key[key.index('/')+1 : second_underscore_index ]
        # Add start date column
        df['start_date'] = key[second_underscore_index + 1 : key.index('.')]
        data.append(df)

    elif key.startswith('Academy/E'):
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        df = pd.read_csv(io.BytesIO(obj['Body'].read()))
        # Course name, including course index
        second_underscore_index = key.index('_', key.index('_') + 1)
        df['course_name'] = key[key.index('/')+1 : second_underscore_index ]
        # Add start date column
        df['start_date'] = key[second_underscore_index + 1 : key.index('.')]
        engineering.append(df)

business_df = pd.concat(business, ignore_index=True)
engineering_df = pd.concat(engineering, ignore_index=True)
data_df = pd.concat(data, ignore_index=True)

# converting all behaviour scores to type int
behaviours = ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']
week_numbers = [f'_W{i}' for i in range(1, 11)]
column_names = [behaviour + week_number for behaviour in behaviours for week_number in week_numbers]

business_df[column_names] = business_df[column_names].astype('Int64')
engineering_df[column_names] = engineering_df[column_names].astype('Int64')
data_df[column_names] = data_df[column_names].astype('Int64')

# converting start date values to date-time
business_df['start_date'] = business_df['start_date'].astype('datetime64[ns]')
engineering_df['start_date'] = engineering_df['start_date'].astype('datetime64[ns]')
data_df['start_date'] = data_df['start_date'].astype('datetime64[ns]')


