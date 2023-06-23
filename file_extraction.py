import boto3


# extracts a set of files of the same format to its associated dictionary key
def extract_file_type(s3, prefix: str, files_dict: dict, file_type: str = None):
    bucket = 'data-eng-228-final-project'
    data = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    content = data['Contents']

    # exports all the academy csv files into the academy_csv dictionary
    if prefix == 'Academy':
        for i in range(0, len(content)):
            file_name = content[i]['Key']
            obj = s3.get_object(Bucket=bucket, Key=file_name)
            files_dict['academy_csv'].append(obj)

    # exports all the Talent files into the corresponding dictionary for each file format
    if prefix == 'Talent':
        # using the paginator class to iterate through pages on s3
        paginator = s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket, Prefix=prefix)

        for page in pages:
            content = page['Contents']

            for i in range(0, len(content)):
                file_name = content[i]['Key']

                if file_name.endswith(file_type):
                    obj = s3.get_object(Bucket='data-eng-228-final-project', Key=file_name)
                    files_dict[file_type].append(obj)


# calls all the functions to extract each file type from the two folders 'Academy' and 'Talent'
def extract_all_files(s3, files_dict: dict):
    extract_file_type(s3, 'Academy', files_dict)
    extract_file_type(s3, 'Talent', files_dict, 'json')
    extract_file_type(s3, 'Talent', files_dict, 'csv')
    extract_file_type(s3, 'Talent', files_dict, 'txt')


# extracts all files from amazon s3, returning a dictionary of files of the same folder and format under the same key
def extract() -> dict:
    s3_client = boto3.client('s3')
    files_dict = {'academy_csv': [], 'json': [], 'txt': [], 'csv': []}

    extract_all_files(s3_client, files_dict)

    return files_dict

