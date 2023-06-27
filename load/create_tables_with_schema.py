# Create a SQLite connection
conn = sqlite3.connect('my_database1.db')
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
        name_id INTEGER PRIMARY KEY,
        name TEXT,
        interview_date TEXT,
        invited_by TEXT,
        address_id INTEGER,
        gender TEXT,
        date_of_birth TEXT,
        email TEXT,
        phone_number TEXT,
        uni_degree TEXT,
        self_development INTEGER,
        interview_result TEXT,
        geoflex INTEGER,
        finacial_support_self INTEGER,
        course_interest TEXT,
        sparta_day_date TEXT,
        psychometric_results INTEGER,
        presentation_results INTEGER,
        sparta_day_location TEXT,
        FOREIGN KEY(address_id) REFERENCES address(address_id)
    )
    ''',
    '''
    CREATE TABLE tech_scores (
        tech_scores_id INTEGER PRIMARY KEY,
        tech_self_scores TEXT
    )
    ''',
    '''
    CREATE TABLE tech_junc (
        name_id INTEGER,
        tech_scores_id INTEGER,
        scores INTEGER,
        PRIMARY KEY (name_id, tech_scores_id),
        FOREIGN KEY(name_id) REFERENCES candidates(name_id),
        FOREIGN KEY(tech_scores_id) REFERENCES tech_scores(tech_scores_id)
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
        name_id INTEGER,
        weakness_id INTEGER,
        PRIMARY KEY (name_id, weakness_id),
        FOREIGN KEY(name_id) REFERENCES candidates(name_id),
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
        name_id INTEGER,
        strength_id INTEGER,
        PRIMARY KEY (name_id, strength_id),
        FOREIGN KEY(name_id) REFERENCES candidates(name_id),
        FOREIGN KEY(strength_id) REFERENCES strength(strength_id)
    )
    ''',
    '''
    CREATE TABLE academy_results (
        name_id INTEGER,
        week_no INTEGER,
        analytic INTEGER,
        independant INTEGER,
        determined INTEGER,
        professional INTEGER,
        imaginative INTEGER,
        PRIMARY KEY (name_id, week_no),
        FOREIGN KEY(name_id) REFERENCES candidates(name_id)
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
    CREATE TABLE Trainers (
        Trainer_id INTEGER PRIMARY KEY,
        Trainer_name TEXT
    )
    ''',
    '''
    CREATE TABLE trainers_junc (
        course_id INTEGER,
        Trainer_id INTEGER,
        PRIMARY KEY (course_id, Trainer_id),
        FOREIGN KEY(course_id) REFERENCES courses(course_id),
        FOREIGN KEY(Trainer_id) REFERENCES Trainers(Trainer_id)
    )
    ''',
    '''
    CREATE TABLE academy (
        name_id INTEGER,
        course_id INTEGER,
        PRIMARY KEY (name_id, course_id),
        FOREIGN KEY(name_id) REFERENCES candidates(name_id),
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
