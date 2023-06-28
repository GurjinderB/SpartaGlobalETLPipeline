from file_extraction import *
from to_dataframe import *
from to_tables import *


def main():
    files_dict, academy_csvs_file_names = extract()
    dataframes = convert(files_dict, academy_csvs_file_names)
    transform_to_tables(dataframes, academy_csvs_file_names)


if __name__ == "__main__":
    main()