from file_extraction import * # take this out
from to_dataframe import * # take this out
from clean_dfs import *
import pandas as pd
from IPython.display import display


def get_person_ids(applicants):
    applicants = applicants.reset_index().reset_index().drop(['index', 'id'], axis=1).rename(
        columns={'level_0': 'person_id'})
    return applicants


def generate_candidates_df(applicants, talents, sparta_day_results):
    applicants_and_talents = applicants.merge(talents,
                                              left_on=['name', 'invite_date'],
                                              right_on=['name', 'date'],
                                              how='left')

    candidates = applicants_and_talents.merge(sparta_day_results,
                                              left_on=['name', 'invite_date'],
                                              right_on=['name', 'date'],
                                              how='left')

    candidates = candidates.drop(candidates.index[candidates.person_id.duplicated().tolist()].tolist())
    candidates.reset_index(drop=True, inplace=True)
    candidates['person_id'] = candidates.index + 1
    return candidates


# makes an address table dataframe
def generate_address_df(df):
    addresses = list()
    # for each row in the talent csv files, add each unique address, city and postcode to a list
    for index, row in df.iterrows():
        address_list = list()
        address_list.append(row["address"])
        address_list.append(row["city"])
        address_list.append(row["postcode"])
        if address_list not in addresses:
            addresses.append(address_list)

    address_table = pd.DataFrame(addresses, columns=["address", "city", "postcode"])
    address_table.index = address_table.index + 1
    address_table['address_id'] = address_table.index
    return address_table


def address_id_in_candidates(candidates, address_table):
    merged = pd.merge(candidates, address_table, on='address')
    merged = merged.drop(columns=['city_x', 'address', 'postcode_x', 'city_y', 'postcode_y', 'date_y'])
    merged = merged.rename(columns={"date_x": "date"})
    return merged


def generate_courses_table(dataframe):
    courses_df = dataframe
    courses_df = pd.DataFrame().assign(course=courses_df['course'], date=courses_df['date'])
    courses_df['course_id'] = courses_df.groupby(['course']).ngroup()
    courses_df = courses_df[['course', 'course_id', 'date']].copy()
    courses_df.set_index('course_id', inplace=True)
    courses_df.groupby(['course', 'date'])
    courses_df = courses_df.drop_duplicates()
    courses_df = courses_df.reset_index()
    courses_df.index = courses_df.index + 1
    courses_df['course_id'] = courses_df.index
    courses_df = courses_df.rename(columns={"course": "course_name"})

    return courses_df


# makes a weakness table dataframe
def generate_weakness_df(df):
    weaknesses_list = list()

    # for each row in the dataframe add distinct weaknesses to a list
    for index, row in df.iterrows():
        for weakness in row["weaknesses"]:
            if weakness not in weaknesses_list:
                weaknesses_list.append(weakness)

    # turn weaknesses list into a dataframe and add a column for the weakness id
    weakness_table = pd.DataFrame(weaknesses_list, columns=['weakness'])
    weakness_table.index = weakness_table.index + 1
    weakness_table['weakness_id'] = weakness_table.index
    return weakness_table


# takes talent jsons, talent csv, weakness dataframe as arguments
def generate_weakness_junc_df(candidates, weakness_df):
    weakness_junc_list = list()

    # for each row in the merged dataframe add (id, weakness) to a new dataframe for each weakness in weaknesses list
    for index, row in candidates.iterrows():
        person_id = row['person_id']
        weaknesses = row['weaknesses']

        if type(weaknesses) == list:
            for weakness in weaknesses:
                weakness_junc_list.append([person_id, weakness])

    weakness_junc = pd.DataFrame(weakness_junc_list, columns=['person_id', 'weakness'])

    weakness_junc = pd.merge(weakness_junc, weakness_df, on='weakness')[['person_id', 'weakness_id']]

    return weakness_junc


def generate_strengths_df(df):
    # Create a new DataFrame for strengths column only
    column_names = ['name', 'strengths']
    strengths = df[column_names]

    # Initialize an empty list to store row data
    strengths_list = []

    # Iterate over all rows
    for index, row in strengths.iterrows():
        strengths_list_row = row['strengths']

        # Iterate over each strength in the list
        for strength in strengths_list_row:
            if strength not in strengths_list:
                # Append row data to the list
                strengths_list.append(strength)

    # Create a new DataFrame from the list
    strengths_df = pd.DataFrame({'strength': strengths_list})
    strengths_df.index = strengths_df.index + 1
    strengths_df['strength_id'] = strengths_df.index
    # Return the new DataFrame
    return strengths_df


# takes talent jsons, talent csv, weakness dataframe as arguments
def generate_strengths_junc_df(candidates, strengths_df):
    strengths_junc_list = list()

    for index, row in candidates.iterrows():
        person_id = row['person_id']
        strengths = row['strengths']

        if type(strengths) == list:
            for strength in strengths:
                strengths_junc_list.append([person_id, strength])

    strengths_junc = pd.DataFrame(strengths_junc_list, columns=['person_id', 'strength'])

    strengths_junc = pd.merge(strengths_junc, strengths_df, on='strength')[['person_id', 'strength_id']]

    return strengths_junc


def generate_tss_df(df, candidates):
    # Create names lists
    names = []
    technologies = []
    self_scores = []

    # Loop through dataframe
    for index, row in df.iterrows():
        try:
            techs = list(row['tech_self_score'].keys())
            for tech in techs:
                names.append(row['name'].upper())
                technologies.append(tech)
                self_scores.append(row['tech_self_score'][tech])
        except AttributeError:
            continue

    tech_self_scores = pd.DataFrame({'name': names, 'technology': technologies, 'self_score': self_scores})
    merged = pd.merge(tech_self_scores, candidates, on='name')[['person_id', 'technology', 'self_score']]

    return merged


def generate_academy_table(df_academy_csv, courses_table, candidates):
    merged = pd.merge(df_academy_csv, courses_table, left_on='course', right_on='course_name')[['name', 'course_id']]
    academy_table = pd.merge(merged, candidates, on='name')[['person_id', 'course_id']]
    return academy_table


def generate_trainers_table(df):
    old_name = "Ely Kely"
    new_name = "Elly Kelly"
    df.loc[:, ["trainer"]] = df.loc[:, ["trainer"]].replace(old_name, new_name)
    trainers_course = df[["trainer", "course", "date"]]
    trainer_course = trainers_course.drop_duplicates(subset = ["date"], keep= "first")
    trainer_course['trainer_id'] = range(1, len(trainer_course) + 1)
    trainers = trainer_course.set_index('trainer_id')
    trainers_table = trainers.drop(['course', 'date'], axis=1).drop_duplicates()
    trainers = trainers.rename(columns={"course": "course_name"})
    trainers['trainer_id'] = trainers.index
    return trainers, trainers_table


def generate_trainers_junc(df_trainers, df_courses):
    trainers_junc = pd.merge(df_trainers, df_courses, on='course_name')[['course_id', 'trainer_id']]
    return trainers_junc


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


def join_person_id_to_acares(candidates, academy_results):
    passed_candidates = candidates[(candidates.result == 'PASS').tolist()]

    academy_results = academy_results.merge(passed_candidates[['person_id', 'name']], left_on='name', right_on='name')
    return academy_results


# should return two lists first one with tables second with
def transform_to_tables(dataframes: dict, academy_csvs_file_names: list) -> (list, list):
    df_academy_csv = dataframes['academy_csv']
    df_academy_csv = clean_academy_csv(df_academy_csv)

    df_csv = dataframes['csv']
    df_csv = clean_talent_csv(df_csv)

    df_json = dataframes['json']
    df_json = clean_talent_json(df_json)

    df_txt = dataframes['txt']
    df_txt = clean_talent_txt(df_txt)

    tables = list()
    table_names = list()

    # generate all the tables to put into the database and add them to a list along with a list of the table names
    candidates = generate_candidates_df(get_person_ids(df_csv), df_json, df_txt)
    address = generate_address_df(df_csv)
    tables.append(address)
    table_names.append('address')

    candidates = address_id_in_candidates(candidates, address)
    tables.append(candidates)
    table_names.append('candidates')

    courses = generate_courses_table(df_academy_csv)
    tables.append(courses)
    table_names.append('courses')

    weaknesses = generate_weakness_df(df_json)
    tables.append(weaknesses)
    table_names.append('weaknesses')

    weakness_junc = generate_weakness_junc_df(candidates, weaknesses)
    tables.append(weakness_junc)
    table_names.append('weakness_junc')

    strengths = generate_strengths_df(df_json)
    tables.append(strengths)
    table_names.append('strengths')

    strengths_junc = generate_strengths_junc_df(candidates, strengths)
    tables.append(strengths_junc)
    table_names.append('strengths_junc')

    tech_scores = generate_tss_df(df_json, candidates)
    tables.append(tech_scores)
    table_names.append('tech_scores')

    academy = generate_academy_table(df_academy_csv, courses, candidates)
    tables.append(academy)
    table_names.append('academy')

    trainers_table, trainers = generate_trainers_table(df_academy_csv)
    tables.append(trainers)
    table_names.append('trainers')

    trainers_junc = generate_trainers_junc(trainers_table, courses)
    tables.append(trainers_junc)
    table_names.append('trainers_junc')

    df = transform_acares(academy_csvs_file_names)
    academy_results = make_wide(df)
    academy_results = join_person_id_to_acares(candidates, academy_results)
    tables.append(academy_results)
    table_names.append('academy_results')

    print(table_names)
    for table in tables:
        display(table)

    return tables, table_names


def main():
    files_dict, academy_csvs_file_names = extract()
    dataframes = convert(files_dict, academy_csvs_file_names)
    transform_to_tables(dataframes, academy_csvs_file_names)
    return


main()
