import pandas as pd
import os
import json
from file_extraction import *


# converts the body of a text file object into a pandas dataframe
def txt_to_pd(obj):
    text = obj['Body'].read().decode('utf-8')

    # Split text
    lines = text.splitlines()

    # Define attributes
    names = []
    psychometrics = []
    presentations = []
    dates = []
    academies = []

    # Transformation loop
    for line in lines:
        this_date = pd.to_datetime(lines[0])
        this_academy = lines[1]

        if line in [lines[0], lines[1], lines[2]]:
            continue
        else:
            name_index = line.index(' -')
            split_name = line[:name_index].split()
            full_name = split_name[0].capitalize() + ' ' + split_name[1].capitalize()
            names.append(full_name)

            psychometrics_start = line.index('Psychometrics: ') + len('Psychometrics: ')
            psychometrics_end = line.index('/100,')
            psychometrics_score = line[psychometrics_start:psychometrics_end]
            psychometrics.append(int(psychometrics_score))

            present_start = line.index('Presentation: ') + len('Presentation: ')
            present_end = line.index('/32')
            present_score = line[present_start:present_end]
            presentations.append(int(present_score))

            academies.append(this_academy)
            dates.append(this_date)

    applicants = {'name': names, 'psychometrics_score': psychometrics, 'presentation_score': presentations,
                  'academy': academies, 'date': dates}

    applicants_pd = pd.DataFrame(applicants)
    return applicants_pd


# converts the contents of a json file object into a pandas dataframe
def json_to_pd(obj):
    json_data = obj['Body'].read().decode('utf-8')
    parsed_json = json.loads(json_data)

    # create a DataFrame from the collected json data
    df = pd.DataFrame([parsed_json])

    return df


# returns a pandas dataframe for any file format of a single file
def convert_to_df(obj, file_type):
    data = []

    if file_type == 'csv':
        data = pd.read_csv(obj['Body'])

    elif file_type == 'json':
        data = json_to_pd(obj)

    elif file_type == 'txt':
        data = txt_to_pd(obj)

    return data


# convert all the files of the same format and folder to a single pandas dataframe
def convert_all_to_df(file_objects: list, file_type: str):
    df = pd.DataFrame()

    # for each file, convert to a dataframe and add it onto the current dataframe
    for obj in file_objects:
        df = pd.concat([df, convert_to_df(obj, file_type)])

    return df


s3_client = boto3.client('s3')
obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key='Academy/Data_28_2019-02-18.csv') # Gurjinder
# print(convert_to_df(obj, 'csv'))

obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key='Talent/10383.json') # Michail
# print(convert_to_df(obj, 'json'))

obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key='Talent/April2019Applicants.csv') # Mankabir
# print(convert_to_df(obj, 'csv'))

obj = s3_client.get_object(Bucket='data-eng-228-final-project', Key='Talent/Sparta Day 1 August 2019.txt') # Fuad
# print(convert_to_df(obj, 'txt'))

obj = s3_client.get_object(Bucket='data-eng-228-final-project', Prefix='Talent/')
print(convert_all_to_df(obj, 'csv'))
