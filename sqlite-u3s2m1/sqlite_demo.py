#!/usr/bin/env python
"""Creating and inserting data with SQLite."""
import sqlite3
def create_table(conn):
    curs = conn.cursor()
    create_statement = """
      CREATE TABLE students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name CHAR(20),
        favorite_number INTEGER,
        least_favorite_number INTEGER
      );
    """
    curs.execute(create_statement)
    curs.close()
    conn.commit()
def insert_data(conn):
    my_data = [
       ('Malven', 7, 12),
       ('Dondre', -5, 101),
       ('Peggy', 14, 74)
    ]
    curs = conn.cursor()
    for row in my_data:
        query = f'INSERT INTO students (name, favorite_number, least_favorite_number) VALUES{row}'
        print(query)
        curs.execute(query)
    curs.close()
    conn.commit()


if __name__ == '__main__':
    conn = sqlite3.connect('example_db.sqlite3')

    # Table names in sqlite3
    curs = conn.cursor()
    res = conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
    print("\n\nTable names in sqlite3:")
    for name in res:
        print(name)
        table_name = 'students'
    query = f'DROP TABLE IF EXISTS {table_name}'
    curs.execute(query)
    curs.close()
    
    create_table(conn)
    insert_data(conn)
