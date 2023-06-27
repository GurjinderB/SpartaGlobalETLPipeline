#!/usr/bin/env python
# coding: utf-8

# In[16]:


import pandas as pd
import os
import json
from file_extraction import *
from to_dataframe import *

s3_client = boto3.client('s3')

files_dict = {'academy_csv': [], 'json': [], 'txt': [], 'csv': []}

academy_csvs_file_names = extract_file_type(s3_client, 'Academy', files_dict, 'csv')

df = convert_all_to_df(files_dict['academy_csv'], 'csv', academy_csvs_file_names)

# Uppercase Function
def make_upper(df, column):
    df[column] = df[column].str.upper()


# In[4]:


display(df)


# In[22]:


# Change name of trainer Ely Kely to Elly Kelly

old_name = "Ely Kely"
new_name = "Elly Kelly"

df.loc[:, ["trainer"]] = df.loc[:, ["trainer"]].replace(old_name, new_name)


# In[23]:


trainers_course = df[["trainer", "course"]]

trainer_course = trainers_course.drop_duplicates(subset = ["trainer"], keep= "first")

make_upper(trainer_course, "trainer")

display(trainer_course)


# In[24]:


trainer_course['trainer_id'] = range(1, len(trainer_course) + 1)

trainers = trainer_course.set_index('trainer_id')

display(trainers)


# In[ ]:




