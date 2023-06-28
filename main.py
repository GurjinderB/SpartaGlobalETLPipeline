from file_extraction import *
from to_dataframe import *
from to_tables import *
from to_database import *


def main():
    files_dict, academy_csvs_file_names = extract()
    dataframes = convert(files_dict, academy_csvs_file_names)
    tables, table_names = transform_to_tables(dataframes, academy_csvs_file_names)
    all_df_to_db(tables, table_names, 'sparta_global_data228.db')
    print("Database Created.")


if __name__ == "__main__":
    main()
