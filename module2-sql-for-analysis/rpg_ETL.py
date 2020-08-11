import pycopg2-binary
dbname = 'wzptmnnk'
user = 'wzptmnnk'
password = 'YBpmqDjZo7ZChd-M7P0y5l9eZpkX6bfV'
host = 'isilo.db.elephantsql.com'

pg_conn = psycopg2.connect(dbname=dbname, user=user,
                           password=password, host=host)
pg_curs = pg_conn.cursor()

# List existing tables
pg_curs.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
existing_tables = pg_curs.fetchall()
print(existing_tables)

# ALternative way for listing the existing tables
pg_curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
for table in pg_curs.fetchall():
    print(table)


# Create a table
create_table_statement = """
CREATE TABLE test_table (
  id SERIAL PRIMARY KEY,
  name varchar(40) NOT NULL,
  data JSONB
);
"""
pg_curs.execute(create_table_statement)
# update the cloud database
pg_conn.commit()

# List all the tables:
pg_curs.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
existing_tables = pg_curs.fetchall()
print(existing_tables)

#ALternatively
pg_curs.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
for table in pg_curs.fetchall():
    print(table)

# Inserting data into the table
insert_statement = """
INSERT INTO test_table (name, data) VALUES
(
  'Zaphod Beeblebrox',
  '{"key": "value", "key2": true}'::JSONB
)
"""
pg_curs.execute(insert_statement)
pg_conn.commit()

# Deleting the newly created table
for i in len(existing_tables):
    dropTableStmt   = f"DROP TABLE {existing_tables[i][0]};"
    pg_curs.execute(dropTableStmt)
pg_conn.commit()




