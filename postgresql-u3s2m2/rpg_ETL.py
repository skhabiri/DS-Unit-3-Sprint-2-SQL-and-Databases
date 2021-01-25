import psycopg2
import os
import sqlite3
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path, verbose=True)
# load_dotenv(verbose=True)

# Defining a function to refresh connection and cursor
def refresh_connection_and_cursor():
    try:
        pg1_curs.close()
        pg1_conn.close()
    except: pass
    pg1_conn = psycopg2.connect(dbname=os.getenv("dbname"), user=os.getenv("user"),
                              password=os.getenv("password"), host=os.getenv("host"))
    pg1_curs = pg1_conn.cursor()
    return pg1_conn, pg1_curs


pg1_conn, pg1_curs = refresh_connection_and_cursor()

# List existing tables in postgreSQL
def pg_table_list1(cursor):
    cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
    tables = cursor.fetchall()
    print(tables)
    return tables


print("*"*20+"\n")
print("All tables in postgres db by pg_table_list1")
pg_table_list1(pg1_curs)
print("*"*20+"\n")


# ALternative way for listing the existing tables in postgreSQL
def pg_table_list2(cursor):
  cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
  for table in cursor.fetchall():
      print(table)

print("get table names in postgres db by pg_table_list2")
pg_table_list2(pg1_curs)
print("*"*20+"\n")

# Deleting prior tables
def pg_table_del(conn, cursor):
    # if len(pg_table_list(cursor)) > 0:
    tables = pg_table_list1(cursor)
    for i in range(len(tables)):
        print(f"dropping {tables[i][0]}")
        dropTableStmt = f"DROP TABLE {tables[i][0]};"
        cursor.execute(dropTableStmt)
    conn.commit()
    print("*"*20+"\n")


print("Deleting prior tables in postgres")
pg_table_del(pg1_conn, pg1_curs)

print("get table names in postgres db by pg_table_list2")
pg_table_list2(pg1_curs)
print("*"*20+"\n")

# Create a table
create_table_statement = """
CREATE TABLE test_table (
  id SERIAL PRIMARY KEY,
  name VARCHAR(40) NOT NULL,
  data JSONB
);
"""
print("create the test_table in postgres")
pg1_curs.execute(create_table_statement)
# save the transaction on the cloud database
pg1_conn.commit()
print("*"*20+"\n")


# List all the postgres tables in the connection

print("All tables in postgres db by pg_table_list1")
pg_table_list1(pg1_curs)
print("*"*20+"\n")

# Inserting data into the table
# json blob keys are always string.
# ::JSONB casts the string into jsonb and it's postgrSQL specific syntax
# name NOT NULL constraint is enforced 
# and would error if null was passed as value instead of zaphod ...
insert_statement = """
INSERT INTO test_table (name, data) VALUES
(
  'Zaphod Beeblebrox',
  '{"key": "value", "key2": true}'::JSONB
)
"""
print("Inserting data into test_table\n")
pg1_curs.execute(insert_statement)
pg1_conn.commit()

# Fetch the inserted data:
pg1_curs.execute('SELECT * FROM test_table;')
print("Content of the test_table:")
print(pg1_curs.fetchall())

# Deleting the newly created table
pg_table_del(pg1_conn, pg1_curs)
print("*"*20+"\n")

print("\n\nNow let's data pipeline from sqlite to postgresql, ETL\n")

# SQLITE3 RPG DATABASE
url = "https://github.com/skhabiri/SQL-Databases-u3s2/blob/master/sqlite-u3s2m1/rpg_db.sqlite3?raw=true"
os.system(f'wget {url}')
os.system('mv rpg_db.sqlite3?raw=true rpg_db.sqlite3')
os.system('ls')

print("Step 1 - Extraction on charactercreator_character table")
sl1_conn = sqlite3.connect('rpg_db.sqlite3')
sl1_curs = sl1_conn.cursor()

# Table names in sqlite3
res = sl1_conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("\n\nTable names in sqlite3:")
for name in res:
    print(name[0])

print("\nget the content of the charactercreator_character")
get_characters = "SELECT * FROM charactercreator_character;"
# characters is a list of tuples. Once fetchall(), the the buffer gets cleared
characters = sl1_curs.execute(get_characters).fetchall()
print("1st row of sqlite db")
print("first row:\n", characters[0])
print("Total number of rows:\n", len(characters))

print("Step 2 - Transformation")
# create Postgres schema

print("\nLet's check sqlite3 schema first to use it for building postgres schema:")
print(sl1_curs.execute('PRAGMA table_info(charactercreator_character);').fetchall())

create_character_table = """
CREATE TABLE charactercreator_character(
  character_id SERIAL PRIMARY KEY,
  name VARCHAR(30),
  level INT,
  exp INT,
  hp INT,
  strength INT,
  intelligence INT,
  dexterity INT,
  wisdom INT
);
"""

# Execute the create table
pg1_curs.execute(create_character_table)
pg1_conn.commit()

# We can query tables if we want to check
def pg_table_list3(cursor):
    show_tables = """
    SELECT * FROM pg_catalog.pg_tables 
    WHERE schemaname != 'pg_catalog'
    AND schemaname != 'information_schema';
    """
    cursor.execute(show_tables)
    return cursor.fetchall()

print("\ncreated tables in postgres with pg_table_list3:")
print(pg_table_list3(pg1_curs))
print("All tables in postgres db by pg_table_list1")
pg_table_list1(pg1_curs)
print("*"*20+"\n")

print("Step 3 - Load!")
#Postgres generates the id field and should be dropped when inserting
example_insert = """
INSERT INTO charactercreator_character
(name, level, exp, hp, strength, intelligence, dexterity, wisdom)
VALUES """ + str(characters[0][1:]) + ";"

# Not running, just inspecting
print("\nexample insert:", example_insert)

print("\nLoading data one at a time...")
for character in characters:
    insert_character = """
    INSERT INTO charactercreator_character
    (name, level, exp, hp, strength, intelligence, dexterity, wisdom)
    VALUES """ + str(character[1:]) + ";"
    pg1_curs.execute(insert_character)

pg1_conn.commit()
pg1_curs.execute('SELECT * FROM charactercreator_character LIMIT 2;')
print("First two rows of data in postgres db")
print(pg1_curs.fetchall())

# Sanity check of transferred data
pg1_curs.execute('SELECT * FROM charactercreator_character;')
pg_characters = pg1_curs.fetchall()

print("\nCheck every row of sqlite3 db vs postgres db")
for character, pg_character in zip(characters, pg_characters):
    assert character == pg_character

# Closing out cursor/connection to wrap up
pg1_curs.close()
pg1_conn.close()
sl1_curs.close()
sl1_conn.close()
