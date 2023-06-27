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


def academy_csv_to_pd(obj, file_name: str):
    df = pd.DataFrame()

    key = file_name
    date_file = key.split(".")[0]
    date_file = ''.join(date_file.split('_')[2])
    df = pd.read_csv(obj['Body'])

    if key.startswith('Academy/B'):
        df.insert(2, "course", "Business")
        df.insert(3, "date", date_file)

    elif key.startswith('Academy/D'):
        df.insert(2, "course", "Data")
        df.insert(3, "date", date_file)

    elif key.startswith('Academy/E'):
        df.insert(2, "course", "Engineering")
        df.insert(3, "date", date_file)

    return df


# returns a pandas dataframe for any file format of a single file
def convert_to_df(obj, file_type: str, file_name: str = None):
    data = []

    if file_name is not None:
        data = academy_csv_to_pd(obj, file_name)

    elif file_type == 'csv':
        data = pd.read_csv(obj['Body'])

    elif file_type == 'json':
        data = json_to_pd(obj)

    elif file_type == 'txt':
        data = txt_to_pd(obj)

    return data


# convert all the files of the same format and folder to a single pandas dataframe
def convert_all_to_df(file_objects: list, file_type: str, academy_csvs_file_names: list = None):
    df = pd.DataFrame()

    # for each file, convert to a dataframe and add it onto the current dataframe
    i = 0
    for obj in file_objects:
        if academy_csvs_file_names is not None:
            df = pd.concat([df, convert_to_df(obj, file_type, academy_csvs_file_names[i])])
        else:
            df = pd.concat([df, convert_to_df(obj, file_type)])
        i += 1

    return df


# converts a dict of file objects into a dict of aggregated pandas dataframes of the same file type and folder
def convert(files_dict: dict, academy_csvs_file_names: list) -> dict:
    academy_csv_df = convert_all_to_df(files_dict["academy_csv"], 'csv', academy_csvs_file_names)
    talent_json_df = convert_all_to_df(files_dict["json"], 'json')
    talent_txt_df = convert_all_to_df(files_dict["txt"], 'txt')
    talent_csv_df = convert_all_to_df(files_dict["csv"], 'csv')
    dataframes = {"academy_csv": academy_csv_df, "json": talent_json_df, "txt": talent_txt_df, "csv": talent_csv_df}
    return dataframes
