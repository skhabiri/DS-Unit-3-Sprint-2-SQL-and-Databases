import pandas as pd
import sqlite3
import psycopg2


data = pd.read_csv("./titanic.csv")
print(data.head())
print(data.shape)
print(f'Number of Nulls: {data.isna().sum(axis=0).sum()}')
print(data.dtypes)

# Search a column for a partial text
"""Sqlite wraps a text with single quote with double quote.
while wraps the other texts with single quote. Postgres only 
sees the single quotes as text and confuses double quotes with 
identifiers. SO we convert ' with space to avoid that.
"""

print(data[data["Name"].str.contains("'")])
data['Name'] = data['Name'].str.replace("'", " ")

# create a sqlite table from the csv file
conn = sqlite3.connect("titanic.sqlite3")
curs = conn.cursor()
data.to_sql(name="mtable", con=conn, if_exists='replace')

# STEP 1 Extraction: get the content of the mtable
# Check the sqlite data
query = "SELECT * FROM mtable"
curs.execute(query)
result = curs.fetchall()
print(result[0])
print(len(result))

# Step 2 - Transformation
# create Postgres schema

# sqlite3 schema
print(curs.execute('PRAGMA table_info(mtable);').fetchall())

# connect to a postgress db
dbname = 'wzptmnnk'
user = 'wzptmnnk'
password = 'YBpmqDjZo7ZChd-M7P0y5l9eZpkX6bfV'
host = 'isilo.db.elephantsql.com'

def refresh_connection_and_cursor():
  try:
    pg_curs.close()
    pg_conn.close()
  except: pass
  pg_conn = psycopg2.connect(dbname=dbname, user=user,
                             password=password, host=host)
  pg_curs = pg_conn.cursor()
  return pg_conn, pg_curs

# List existing tables
def pg_table_list(cursor):
    cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
    return cursor.fetchall()

pg_conn, pg_curs = refresh_connection_and_cursor()

print(pg_table_list(pg_curs))

table_list=["mtable"]

# Deleting tables in table_list
def pg_table_del(conn, cursor, table_list):
    for i in range(len(pg_table_list(cursor))):
        if pg_table_list(cursor)[i][0] in table_list:
            dropTableStmt = f"DROP TABLE {pg_table_list(cursor)[i][0]};"
            cursor.execute(dropTableStmt)
    conn.commit()

pg_table_del(pg_conn, pg_curs, table_list)

create_mtable = """
CREATE TABLE mtable(
  passenger_id SERIAL PRIMARY KEY,
  Survived INT,
  PClass INT,
  Name TEXT,
  Sex TEXT,
  Age REAL,
  Siblings_Spouses_Aboard INT,
  Parents_Children_Abroad INT,
  Fare FLOAT);
"""

# Execute the create table
pg_curs.execute(create_mtable)

#Change starting sequence
# query = "ALTER SEQUENCE mtable_passenger_id_seq RESTART WITH 10 INCREMENT BY 2;"
# pg_curs.execute(query)

# pg_conn.commit()

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
pg_curs.execute(show_tables)
print("created tables in postgres:")
print(pg_curs.fetchall())

# Step 3 Insert into the table
example_insert = """
INSERT INTO mtable
(Survived, PClass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Abroad, Fare)
VALUES """ + str(result[0][1:]) + ";"

print("example insert:", example_insert)

idx = 0
for item in result:
  idx +=1
  insert_item = """
    INSERT INTO mtable
    (Survived, PClass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Abroad, Fare)
    VALUES """ + str(item[1:]) + ";"
  pg_curs.execute(insert_item)
  # pg_curs.execute(f'SELECT * FROM mtable WHERE passenger_id = {idx};')
  # pg_item = pg_curs.fetchall()
  # print(pg_item)

# Check every row of sqlite3 db vs postgres db
pg_curs.execute('SELECT * FROM mtable;')
pg_result = pg_curs.fetchall()

for item, pg_item in zip(result, pg_result):
    if item[1:] != pg_item[1:]:
        print(item)
        print(pg_item)

# Closing out cursor/connection to wrap up
pg_curs.close()
pg_conn.close()
curs.close()
conn.close()