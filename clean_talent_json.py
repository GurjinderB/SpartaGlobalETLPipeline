def clean_talent_json(talent_json_df):
    
    # Filling Null values with a string 'N/A'
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
    mapping = {'Yes': True, 'No': False, 'Pass': True, 'Fail': False}
    talent_json_df[convert_clm] = talent_json_df[convert_clm].replace(mapping)
    
    # Applying a fucntion to dictionaries
    talent_json_df = talent_json_df.applymap(update_dict_format)

    # Replacing inconsistencies in dates
    talent_json_df['date'] = pd.to_datetime(talent_json_df['date'], dayfirst=True)
    talent_json_df['date'] = talent_json_df['date'].str.replace('//', '/')

# Defininf a function to check for dictionaries and change the data types inside
def update_dict_format(df_dict):
    if isinstance(df_dict, dict):
        # Converting all keys to capitalised strings and values to integers
        return {str(k).upper(): int(v) for k, v in df_dict.items()}
    return df_dict


