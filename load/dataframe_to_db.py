import sqlite3

#takes in a df and import its to table in a databse .db file
def df_to_db(df: pd.DataFrame, table_name: str, database_name: str):
    con = sqlite3.connect(database_name)
    df.to_sql(table_name, con, if_exists='replace', index=False)
    con.close()    
    
    
#takes in a list of df names and table names and imports the dataframes to specific tables    
def all_df_to_db(df_list: [], table_names:[], database_name:str):
    for df, table_name in zip(df_list, table_names):
        df_to_db(df, table_name, database_name)