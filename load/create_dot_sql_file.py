import sqlite3

# create a .sql file using .db file
def create_sql_file(database_name: str, dot_sql_file: str):
    conn = sqlite3.connect(database_name)
    cursor = conn.cursor()


    with open(dot_sql_file, 'a') as f:
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()

        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_table_sql = cursor.fetchone()[0]
            f.write(f"{create_table_sql};\n")

            cursor.execute(f"SELECT * FROM {table_name};")
            rows = cursor.fetchall()
            for row in rows:
                insert_sql = f"INSERT INTO {table_name} VALUES {row};"
                f.write(f"{insert_sql}\n")

    conn.close()
