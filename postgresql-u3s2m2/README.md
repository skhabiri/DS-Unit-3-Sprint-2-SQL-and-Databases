# SQL for Analysis

SQL is simple, but surprisingly powerful - many data pipelines start, and even
end, with it.

## Objectives

- deploy and use a simple PostgreSQL database
- Create a data pipeline with SQL

## Preparation

Sign up for a free [ElephantSQL](https://www.elephantsql.com/) account. This
will allow you to run a "cloud" PostgreSQL instance (20mb). If you wish to you
may also install [PostgreSQL](https://www.postgresql.org/) locally, which would
facilitate larger databases, but it is not necessary for the daily tasks.

You can also install [pgAdmin](https://www.pgadmin.org/), which (like the DB
Browser for SQLite) lets you connect to, explore, and query databases using a
GUI tool. This is optional, but can be pretty handy.

## Task

In the previous module we used a simple local workflow with SQLite - today, we'll work on
inserting the same RPG data into a more production-style PostgreSQL database
running on a server. We will use [psycopg](http://initd.org/psycopg/), a Python
library for connecting to PostgreSQL, and specifically we will install
[psycopg2-binary](https://pypi.org/project/psycopg2-binary/).

Once we get the data inserted, we will continue exploring querying as yesterday,
first answering the same questions and then going deeper. We'll also explore
some of the specific functions that are different in PostgreSQL than SQLite.

## More to do

Set up and insert the RPG data into a PostgreSQL database.

Then, set up a new table for the Titanic data (`titanic.csv`) - spend some time
thinking about the schema to make sure it is appropriate for the columns.
[Enumerated types](https://www.postgresql.org/docs/9.1/datatype-enum.html) may
be useful. Once it is set up, write a `insert_titanic.py` script that uses
`psycopg2` to connect to and upload the data from the csv, and add the file to
your repo. Then start writing PostgreSQL queries to explore the data!

## Resources and Stretch Goals

PostgreSQL is a real true powerful production database - explore the [official
documentation](https://www.postgresql.org/docs/) as well as larger hosted
offerings such as [Amazon RDS](https://aws.amazon.com/rds/postgresql/).
 
Try to install and use the actual [psycopg2](https://pypi.org/project/psycopg2/)
package (as opposed to `psycop2-binary`) - this builds from source, so there are
[prerequisites](http://initd.org/psycopg/docs/install.html#install-from-source)
you'll need. This may be good to do inside a container!

Want to try larger PostgreSQL databases? Check out [these sample
databases](https://community.embarcadero.com/article/articles-database/1076-top-3-sample-databases-for-postgresql),
but note you'll probably need a local installation of PostgreSQL to be able to
use them.

you can also revisit
[Django](https://docs.djangoproject.com/en/2.1/intro/) . It is a powerful and
widely-used web application framework. Also, the Django ORM can connect to a
variety of SQL backends, and a very typical setup is to use SQLite for (initial)
local development but PostgreSQL for deployment.
____

# Module2: PostgreSQL
**ElephantSQL:**
ElephantSQL is a PostgrSQL as a service offering interface and tooling to work with a storage on a cloud hosting datacenter such as AWS, Google or Azure.


**Steps to follow:**
* Create an instance in ElephantSQL.
* Create a Table by running this query on BROWSER interface:
   * CREATE TABLE test_table (
        id SERIAL PRIMARY KEY,
        name varchar(40) NOT NULL,
        Data JSONB
        );
* Insert some data. :: cast the string into json blob. Json key needs to be a string
   * INSERT INTO test_table (name, data) VALUES
                (‘A row name’, null), 
(‘2nd row with JSON’, ‘{“a”: 1, “b”: [“dog”, “cat”, 42], “c”: true}” :: JSONB);
* Read the data back:
   * SELECT * FROM test_table;
* Now we can connect to the postgresql instance created by ElephantSQL from a colab notebook or a local python file, by installing psycopg2 library
   * pipenv install psycopg2-binary
* Since the database is over the network we need db_name, user, password and host name.
   * pg_conn = psycopg2.connect(dbname, user, password, host)
   * pg_curs = pg_conn.cursor()
   * pg_curs.execute(“SELECT * FROM test_table;”)
   * pg_curs.fetchall()
   * pg_conn.commit()
* Unlike sqlite in postgreSQL the constraints are enforced.
   * pg_curs().execute(“INSERT INTO test_table (name, data) VALUES (null, null);”)
   * pg_curs.close()


**Data Pipeline:**
An ETL Pipeline refers to a set of processes extracting data from an input source, transforming the data, and loading into an output destination such as a database. We are going to extract ‘rpg’ data from sqlite database and transform and load it onto a postgreSQL database.
Steps to follow
* Import rpg_db.sqlite3 database to the colab
   * !wget “url_raw”
   * !mv ‘rpg_db.sqlite3?raw=true’ rpg_db.sqlite3
   * !ls
* Extract charcatercreator_character table from sqlite3
   * Import sqlite3
   * sl_conn = sqlite3.connect(‘rpg_db.sqlite3’)
   * sl_curs = sl_conn.cursor()
   * get_charcaters = “SELECT * FROM charactercreator_character;”
   * sl_curs.execute(get_characters)
   * characters = sl_curs.fetchall()        # It’s a list of tuples
   * len(characters)
* To transform, we need to make a schema in postgreSQL that fits this data. For that we first check the old schema of sqlite.
   * sl_curs.execute(“PRAGMA table_info(charactercreator_character);”)
   * sl_curs.fetchall()
* It's a bunch of integers and varchar. We’ll create a statement in postgres that captures this.
   * create_character_table = “””CREATE TABLE charcatercreator_character
(character_id SERIAL PRIMARY KEY,  name VARCHAR(30), level INT, exp INT, hp   INT, strength INT, intelligence INT, dexterity INT, wisdom INT);”””
   * pg_curs = pg_conn.cursor()
   * pg_curs.execute(create_character_table)
   * pg_conn.commit()
* inquiry the internal of postgres;
   * show_tables = “”” SELECT * FROM pg_catalog.pg_tables WHERE schemaname != ‘pg_catalog’ AND schemaname != ‘information_schema’; “””
   * pg_curs.execute(show_tables)
   * pg_curs.fetchall()
* For loading we want to insert the tuple in a string w/ INSERT INTO but we don’t want the first field (id as postgreSQL generates that.
   * characters[0]
   * example_insert = “”” INSERT INTO charactercreator_character (name, level, exp, hp, strength, intelligence, dexterity, wisdom) VALUES ””” + str(characters[0][1:]) + “;”
   * print(example_insert)                # check the query
   * for character in characters: …        # for loop over the query and execute each time
   * pg_conn.commit()
* above we are executing each row one at a time which is inefficient. All queries between to commit are transactions if any query goes bad anywhere in a transaction the whole transaction might be discarded. so it’s good to commit after INSERT INTO.
   *  pg_curs.execute(‘SELECT * FROM charactercreator_character LIMIT 5;’)
   * pg_curs.fetchall()
* ids might be different depending on whether we had an aborted run or not. If that needs to be fixed we’ll drop the table and schema
   * pg_curs.execute(‘DROP TABLE charactrecreator_character;’)
   * pg.conn.commit()
* How to check the sanity of data
   * pg_curs.execute(‘SELECT * FROM charactercreator_character;’)
   * pg_characters = pg_curs.fetchall()
   * for char, pg_char in zip(characters, pg_characters)
assert char == pg_char
* Closing up the cursor and connection
   * pg_curs.close()
   * pg_conn.close()
If you are working with vscode, `pipenv shell; pip install pylint` to enable linter in vscode as a dev package.
**Libraries:**
```
import psycopg2
import sqlite3
import os
from dotenv import load_dotenv
from pathlib import Path
```
**File structure:**
* rpg_db.sqlite3 is a local sqlite db containing multiple tables that are generated for role play characters. It includes character names, items , weapons spread over multiple tables.
   * rpg_ETL.py is a python file that data pipelines the sqlite onto a postgreSQL db provided by ElephantSQL web service. All steps of Extraction, Transformation, and Loading are performed.
* titanic.csv is a dataset of 900 rows of titanic passengers with features including name, class, children, sex, and so on.
   * insert_titanic.py imports csv into a sqlite db, titanic.sqlite3. The schema of the sqlite is used to create a table with similar data types in postgres. Before that any previous table of that is dropped for a clean start. Data is read from sqlite and loaded into postgres and finally read back from postgres to compare with original data in sqlite for sanity check. Finally some business questions are converted to queries and executed on the pg db.