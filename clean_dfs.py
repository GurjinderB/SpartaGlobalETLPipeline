import pandas as pd


def clean_talent_txt(talent_txt_df):
    # Copy the dataframe to avoid changing the original
    df_cleaned = talent_txt_df.copy()    

    df_cleaned['name'] = df_cleaned['name'].str.upper()
    
    return df_cleaned


def clean_talent_csv(talent_csv_df):
    # Copy the dataframe to avoid changing the original
    df_cleaned = talent_csv_df.copy()

    # Convert 'name' column to string and to uppercase
    df_cleaned['name'] = df_cleaned['name'].astype(str).str.upper()

    # Replace 'Male' and 'Female' with 'M' and 'F' in 'gender' column
    replace_values = {'Male': 'M', 'Female': 'F'}
    df_cleaned['gender'] = df_cleaned['gender'].replace(replace_values)

    # Convert 'dob' column to datetime
    df_cleaned['dob'] = pd.to_datetime(df_cleaned['dob'], format='%d/%m/%Y', errors='coerce')

    # Combine 'column3' and 'column4' into 'combined' column
    df_cleaned['invite_date'] = df_cleaned['invited_date'].astype(str) + df_cleaned['month']
    
    # Remove 'column3' and 'column4'
    df_cleaned = df_cleaned.drop(columns=['invited_date', 'month'])

    # Clean 'invite_date' column
    df_cleaned['invite_date'] = df_cleaned['invite_date'].apply(convert_to_date)

    # Replace degree results
    replacements = {'1st': 1.0, '2:1': 2.1, '2:2': 2.2, '3rd': 3.0}
    df_cleaned['degree'] = df_cleaned['degree'].replace(replacements)

    # Format phone numbers
    df_cleaned['phone_number'] = df_cleaned['phone_number'].str.replace(r'\D', '', regex=True)

    # Convert invited_by to uppercase
    df_cleaned['invited_by'] = df_cleaned['invited_by'].str.upper()

    return df_cleaned


def convert_to_date(date_str):
    if pd.isnull(date_str):  # preserve NaN values
        return pd.NaT
    # Split into day and month/year
    day, month_year = date_str.split('.')  
    # Convert day to int
    day = int(float(day))
    # Remove the '0' at the beginning of the month
    month_year = month_year[1:]  
    # Separate month and year
    month, year = month_year.split(' ')  
    # Convert month abbreviations to full names
    month = month.upper()
    if month == 'JAN':
        month = 'JANUARY'
    elif month == 'FEB':
        month = 'FEBRUARY'
    elif month == 'MAR':
        month = 'MARCH'
    elif month == 'APR':
        month = 'APRIL'
    elif month == 'MAY':
        month = 'MAY'
    elif month == 'JUN':
        month = 'JUNE'
    elif month == 'JUL':
        month = 'JULY'
    elif month == 'AUG':
        month = 'AUGUST'
    elif month == 'SEP' or month == 'SEPT':
        month = 'SEPTEMBER'
    elif month == 'OCT':
        month = 'OCTOBER'
    elif month == 'NOV':
        month = 'NOVEMBER'
    elif month == 'DEC':
        month = 'DECEMBER'
    # Create new date string
    new_date_str = f'{day} {month} {year}' 
    # Convert to datetime
    return pd.to_datetime(new_date_str, format='%d %B %Y')  


# Academy
def clean_academy_csv(academy_csv_df):
    # converting all scores to int
    behaviours = ['Analytic', 'Independent', 'Determined', 'Professional', 'Studious', 'Imaginative']
    week_numbers = [f'_W{i}' for i in range(1, 11)]
    column_names = [behaviour + week_number for behaviour in behaviours for week_number in week_numbers]
    academy_csv_df[column_names] = academy_csv_df[column_names].astype('Int64')
    return academy_csv_df


# Talent
def clean_talent_json(talent_json_df):
    
    # Filling Null values with a string N/A
    talent_json_df = talent_json_df.fillna('N/A')

    # Converting columns values to strings and capitalizing them
    convert_clm = [
        'name', 'self_development', 'geo_flex',
        'financial_support_self', 'result', 'course_interest'
    ]
    
    talent_json_df[convert_clm] = talent_json_df[convert_clm].astype(str).apply(lambda x: x.str.upper())

    # Converting 'self_development', 'geo_flex', 'financial_support_self', 
    # 'result' to boolean values
    convert_clm = ['self_development', 'geo_flex', 'financial_support_self', 'result']
    mapping = {'YES': True, 'NO': False, 'PASS': True, 'FAIL': False}
    talent_json_df[convert_clm] = talent_json_df[convert_clm].replace(mapping)
    
    # Applying a function to dictionaries
    talent_json_df = talent_json_df.applymap(update_dict_format)

    # Replacing inconsistencies in dates
    talent_json_df['date'] = talent_json_df['date'].str.replace('//', '/')
    talent_json_df['date'] = pd.to_datetime(talent_json_df['date'], dayfirst=True)
    return talent_json_df


# Define a function to check for dictionaries and change the data types inside
def update_dict_format(df_dict):
    if isinstance(df_dict, dict):
        # Converting all keys to capitalised strings and values to integers
        return {str(k).upper(): int(v) for k, v in df_dict.items()}
    return df_dict
