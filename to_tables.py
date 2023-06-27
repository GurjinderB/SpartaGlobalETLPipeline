from file_extraction import *
from to_dataframe import *
import pandas as pd
import boto3


def use(key: str):
    s3 = boto3.client('s3')
    obj = s3.get_object(Bucket='data-eng-228-final-project', Key=key)
    filetype = key[key.index('.'):]
    if filetype == '.csv':
        data = pd.read_csv(obj['Body'])
    elif filetype == '.json':
        data = json.load(obj['Body'])
    elif filetype == '.txt':
        data = obj['Body'].read().decode('utf-8')
    return data


def make_wide(table):
    academy_results_wide = table.pivot(index=['name', 'week_no'],
                                       columns='attribute',
                                       values='score')
    academy_results = academy_results_wide.reset_index()
    return academy_results


def make_upper(df, attribute: str='name'):
    return df[attribute].transform(lambda attr: attr.upper())


def transform_acares(keystrings: list):
    tables = []

    for keystring in keystrings:
        current_ar = use(keystring)
        current_ar['name'] = make_upper(current_ar, 'name')
        raw_attributes = current_ar.columns[2:]

        names = []
        week_numbers = []
        attributes = []
        scores = []

        for name in current_ar['name']:
            for attribute in raw_attributes:
                names.append(name)
                week_numbers.append(attribute[attribute.index('W') + 1:])
                attributes.append(attribute[:attribute.index('_')])
                scores.append(float(current_ar[current_ar['name'] == name][attribute].tolist()[0]))
        academy_results_long = pd.DataFrame({'name': names,
                                             'week_no': week_numbers,
                                             'attribute': attributes,
                                             'score': scores})
        tables.append(academy_results_long)
    return pd.concat(tables)


def transform():
    return


def main():
    s3_client = boto3.client('s3')
    files_dict = {'academy_csv': [], 'json': [], 'txt': [], 'csv': []}

    academy_csvs_file_names = extract_file_type(s3_client, 'Academy', files_dict, 'csv')
    df = convert_all_to_df(files_dict['academy_csv'], 'csv', academy_csvs_file_names)
    df1 = transform_acares(academy_csvs_file_names)
    df2 = make_wide(df1)
    print(df2)

    return


main()