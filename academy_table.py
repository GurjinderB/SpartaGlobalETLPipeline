import pandas as pd
import boto3

def academy_table():
    
    s3_client = boto3.client('s3')
    
    # Initialize an empty list to store data
    course_table = []

    # List the objects in the S3 bucket
    response = s3_client.list_objects(Bucket="data-eng-228-final-project", Prefix="Academy")

    # Iterate through the objects in the S3 bucket
    for obj in response['Contents']:
        file_key = obj['Key']
        
        # Extract the course name from the file key
        file_name = file_key.split('/')[-1].split('.')[0].split('_')[:2]
        course_name = file_name[0] + '_' + file_name[1]
        course_group = file_name[0]
        course_date = file_key.split('/')[-1].split('.')[0].split('_')[-1:]
        
        # Read the CSV file from S3 and extract student IDs
        csv_object = s3_client.get_object(Bucket="data-eng-228-final-project", Key=file_key)
        df = pd.read_csv(csv_object['Body'])
        student_ids = df['name'].tolist()
        
        # Append the data to the list
        course_table.extend([(student_id, course_name, course_group, course_date[0]) for student_id in student_ids])

    # Create the DataFrame
    course_df = pd.DataFrame(course_table, columns=['name', 'course_name', 'course_group','course_date'])
    course_df['name'] = course_df['name'].str.capitalize()    
    
    # Return the DataFrame
    return course_df


def course_table(academy_table):
    course_df = academy_table['course_name'].unique()
    course_df = pd.DataFrame(course_df, columns=['course_name'])
    course_df = course_df.rename_axis('course_id')
    return course_df


# Need to pass only candidate_df where 'name_id' can be pulled
def academy_table_df(candidates, git course_table=course_table(academy_table()), academy_table=academy_table()):
    academy = []

    for index, row in candidates.iterrows():
        # Assigning variables to values in candidate table
        inv_date = row['date']
        interest = row['course_interest']
        # 'name' to be changed to 'name_iD' from the candidates table
        candidate_id = row['name_id']
        
        if row['result'] == 'Pass':
            # Keeping only the group that is candidates 'interest'
            grouped_courses = academy_table.groupby('course_group').get_group(interest)
            # Keeping only groups that has start dates after candidates interview
            start_date = grouped_courses[grouped_courses['course_date'] > inv_date]
            # Taking the first group after sorting them in ascending order
            candidate_course = start_date.sort_values('course_date', ascending=True).iloc[0, 1]
            # Adding candidates_id and course_name to the list
            academy.append([candidate_course, candidate_id])
            academy_df = pd.DataFrame(academy, columns=['course_name', 'name_id'])
            
    course_table['course_id'] = course_table.index
    merged_df = pd.merge(course_table, academy_df,  on='course_name', how='inner')
    academy = merged_df.set_index('course_id')
    
    return academy['name_id']

    

