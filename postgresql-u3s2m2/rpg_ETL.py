import psycopg2
import os
import sqlite3

dbname = 'wzptmnnk'
user = 'wzptmnnk'
password = 'YBpmqDjZo7ZChd-M7P0y5l9eZpkX6bfV'
host = 'isilo.db.elephantsql.com'

# Defining a function to refresh connection and cursor
def refresh_connection_and_cursor():
  try:
    pg1_curs.close()
    pg1_conn.close()
  except: pass
  pg1_conn = psycopg2.connect(dbname=dbname, user=user,
                             password=password, host=host)
  pg1_curs = pg1_conn.cursor()
  return pg1_conn, pg1_curs


pg1_conn, pg1_curs = refresh_connection_and_cursor()

# List existing tables
def pg_table_list(cursor):
    cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
    return cursor.fetchall()

# ALternative way for listing the existing tables
pg1_curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
for table in pg1_curs.fetchall():
    print(table)

# Deleting prior tables
def pg_table_del(conn, cursor):
    # if len(pg_table_list(cursor)) > 0:
    for i in range(len(pg_table_list(cursor))):
        dropTableStmt = f"DROP TABLE {pg_table_list(cursor)[i][0]};"
        cursor.execute(dropTableStmt)
    conn.commit()

pg_table_del(pg1_conn, pg1_curs)

# Create a table
create_table_statement = """
CREATE TABLE test_table (
  id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  data JSONB
);
"""
pg1_curs.execute(create_table_statement)
# update the cloud database
pg1_conn.commit()

# List all thee postgres tables in the connection
pg_table_list(pg1_curs)

# Inserting data into the table
insert_statement = """
INSERT INTO test_table (name, data) VALUES
(
  'Zaphod Beeblebrox',
  '{"key": "value", "key2": true}'::JSONB
)
"""
pg1_curs.execute(insert_statement)
pg1_conn.commit()

# Fetch the inserted data:
pg1_curs.execute('SELECT * FROM test_table;')
print(pg1_curs.fetchall())

# Deleting the newly created table
pg_table_del(pg1_conn, pg1_curs)

# SQLITE3 RPG DATABASE
url = "https://github.com/skhabiri/DS-Unit-3-Sprint-2-SQL-and-Databases/blob/master/module1-introduction-to-sql/rpg_db.sqlite3?raw=true"
os.system(f'wget {url}')
os.system('mv rpg_db.sqlite3?raw=true rpg_db.sqlite3')
os.system('ls')

#Step 1 - Extraction on charactercreator_character table
sl1_conn = sqlite3.connect('rpg_db.sqlite3')
sl1_curs = sl1_conn.cursor()

# Table names in sqlite3
res = sl1_conn.execute("SELECT name FROM sqlite_master WHERE type='table';")
print("*******************\nTable names in sqlite3:")
for name in res:
    print(name[0])

# get the content of the charactercreator_character
get_characters = "SELECT * FROM charactercreator_character;"
characters = sl1_curs.execute(get_characters).fetchall()
print(characters[0])
print(len(characters))

# Step 2 - Transformation
# create Postgres schema

# sqlite3 schema
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
show_tables = """
SELECT
   *
FROM
   pg_catalog.pg_tables
WHERE
   schemaname != 'pg_catalog'
AND schemaname != 'information_schema';
"""
pg1_curs.execute(show_tables)
print("created tables in postgres:")
print(pg1_curs.fetchall())

# Step 3 - Load!
print(characters[0])

#Postgres generates the id fild and should be dropped when inserting

example_insert = """
INSERT INTO charactercreator_character
(name, level, exp, hp, strength, intelligence, dexterity, wisdom)
VALUES """ + str(characters[0][1:]) + ";"

print("example insert:", example_insert)  # Not running, just inspecting

for character in characters:
  insert_character = """
    INSERT INTO charactercreator_character
    (name, level, exp, hp, strength, intelligence, dexterity, wisdom)
    VALUES """ + str(character[1:]) + ";"
  pg1_curs.execute(insert_character)

pg1_conn.commit()
pg1_curs.execute('SELECT * FROM charactercreator_character LIMIT 2;')
print(pg1_curs.fetchall())

# Check every row of sqlite3 db vs postgres db
pg1_curs.execute('SELECT * FROM charactercreator_character;')
pg_characters = pg1_curs.fetchall()

for character, pg_character in zip(characters, pg_characters):
  assert character == pg_character

# Closing out cursor/connection to wrap up
pg1_curs.close()
pg1_conn.close()
sl1_curs.close()
sl1_conn.close()