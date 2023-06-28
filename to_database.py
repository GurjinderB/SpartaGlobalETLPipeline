import sqlite3


def df_to_db(df, table_name: str, database_name: str):
    con = sqlite3.connect(database_name)
    df.to_sql(table_name, con, if_exists='append', index=False)
    con.close()


def all_df_to_db(df_list: [], table_names: [], database_name: str):
    for df, table_name in zip(df_list, table_names):
        df_to_db(df, table_name, database_name)


def create_database():
    # Create a SQLite connection
    conn = sqlite3.connect('sparta_global_data228.db')
    c = conn.cursor()

    # Enable foreign key constraint
    c.execute("PRAGMA foreign_keys = ON;")

    # Define the schema as a list of strings
    schemas = [
        '''
        CREATE TABLE address (
            address_id INTEGER PRIMARY KEY,
            address TEXT,
            city TEXT,
            postcode TEXT
        )
        ''',
        '''
        CREATE TABLE candidates (
            person_id INTEGER PRIMARY KEY,
            name TEXT,
            invite_date TEXT,
            invited_by TEXT,
            address_id INTEGER,
            gender TEXT,
            dob TEXT,
            email TEXT,
            phone_number TEXT,
            uni TEXT,
            degree INTEGER,
            self_development INTEGER,
            result TEXT,
            geo_flex INTEGER,
            financial_support_self INTEGER,
            course_interest TEXT,
            sparta_day_date TEXT,
            psychometrics_score INTEGER,
            presentation_score INTEGER,
            sparta_day_location TEXT,
            date TEXT,
            academy TEXT,
            FOREIGN KEY(address_id) REFERENCES address(address_id)
        )
        ''',
        '''
        CREATE TABLE tech_scores (
            person_id INTEGER,
            tech_scores_id INTEGER,
            scores INTEGER,
            PRIMARY KEY (person_id, tech_scores_id),
            FOREIGN KEY(person_id) REFERENCES candidates(person_id)
        )
        ''',
        '''
        CREATE TABLE weakness (
            weakness_id INTEGER PRIMARY KEY,
            weakness TEXT
        )
        ''',
        '''
        CREATE TABLE weakness_junc (
            person_id INTEGER,
            weakness_id INTEGER,
            PRIMARY KEY (person_id, weakness_id),
            FOREIGN KEY(person_id) REFERENCES candidates(person_id),
            FOREIGN KEY(weakness_id) REFERENCES weakness(weakness_id)
        )
        ''',
        '''
        CREATE TABLE strength (
            strength_id INTEGER PRIMARY KEY,
            strength TEXT
        )
        ''',
        '''
        CREATE TABLE strength_junc (
            person_id INTEGER,
            strength_id INTEGER,
            PRIMARY KEY (person_id, strength_id),
            FOREIGN KEY(person_id) REFERENCES candidates(person_id),
            FOREIGN KEY(strength_id) REFERENCES strength(strength_id)
        )
        ''',
        '''
        CREATE TABLE academy_results (
            person_id INTEGER,
            week_no INTEGER,
            analytic INTEGER,
            independant INTEGER,
            determined INTEGER,
            professional INTEGER,
            imaginative INTEGER,
            PRIMARY KEY (person_id, week_no),
            FOREIGN KEY(person_id) REFERENCES candidates(person_id)
        )
        ''',
        '''
        CREATE TABLE courses (
            course_id INTEGER PRIMARY KEY,
            course_name TEXT,
            date TEXT
        )
        ''',
        '''
        CREATE TABLE trainers (
            trainer_id INTEGER PRIMARY KEY,
            trainer_name TEXT
        )
        ''',
        '''
        CREATE TABLE trainers_junc (
            course_id INTEGER,
            trainer_id INTEGER,
            PRIMARY KEY (course_id, trainer_id),
            FOREIGN KEY(course_id) REFERENCES courses(course_id),
            FOREIGN KEY(trainer_id) REFERENCES Trainers(trainer_id)
        )
        ''',
        '''
        CREATE TABLE academy (
            person_id INTEGER,
            course_id INTEGER,
            PRIMARY KEY (person_id, course_id),
            FOREIGN KEY(person_id) REFERENCES candidates(person_id),
            FOREIGN KEY(course_id) REFERENCES courses(course_id)
        )
        '''
    ]

    # Execute each schema
    for schema in schemas:
        c.execute(schema)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()
