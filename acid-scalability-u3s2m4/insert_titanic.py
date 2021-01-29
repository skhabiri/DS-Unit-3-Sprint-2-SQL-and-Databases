import pandas as pd
import sqlite3
import psycopg2

# connect to a postgress db on https://customer.elephantsql.com/instance
dbname = 'wzptmnnk'
user = 'wzptmnnk'
password = 'YBpmqDjZo7ZChd-M7P0y5l9eZpkX6bfV'
host = 'isilo.db.elephantsql.com'

# Import csv into sqlite
def csv_to_sqlite():
    data = pd.read_csv("./titanic.csv")
    print(data.head())
    print(data.shape)
    print(f'Number of Nulls: {data.isna().sum(axis=0).sum()}')
    print(data.dtypes)

    # Search a column for a partial text
    """Sqlite wraps a text containing a single quote with double quotes instead of single quotes.
    While it wraps the other texts which don't have any single quote with single quotes. 
    Transferring this, Postgres only recognizes the single quotes as text and confuses a text wrapped in
    double quotes with identifiers. Hence it gives an error that the column item is missing.
    So we convert any text containing ' with a space or dash line to avoid the above issue. ex/ O'Neill
    """
    print("Names with single quote ' :")
    print(data[data["Name"].str.contains("'")][:1])
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
    print(f'Frirst line of sqlite-mtable:\n{result[0]}')
    print(f'Record length: {len(result)}')

    # sqlite3 schema (INTEGER, TEXT, REAL)
    print("sqlite schema:\n")
    print(curs.execute('PRAGMA table_info(mtable);').fetchall())
    conn.commit()
    curs.close()
    conn.close()
    return result



def refresh_connection_and_cursor():
    try:
        pg_curs.close()
        pg_conn.close()
    except: pass
    pg_conn = psycopg2.connect(dbname=dbname, user=user, password=password, host=host)
    pg_curs = pg_conn.cursor()
    return pg_conn, pg_curs


# List existing tables
def pg_table_list(cursor):
    cursor.execute("SELECT relname FROM pg_class WHERE relkind='r' AND relname !~ '^(pg_|sql_)';")
    return cursor.fetchall()


# Drop tables in table_list
def pg_table_drop(cursor, table_list):
    for i in range(len(pg_table_list(cursor))):
        if pg_table_list(cursor)[i][0] in table_list:
            dropTableStmt = f"DROP TABLE {pg_table_list(cursor)[i][0]};"
            cursor.execute(dropTableStmt)


# Step 2 - Transformation
# create Postgres schema
def create_pgtable(curs):
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
    curs.execute(create_mtable)

    #Change starting sequence of identifier
    # query = "ALTER SEQUENCE mtable_passenger_id_seq RESTART WITH 10 INCREMENT BY 2;"
    # pg_curs.execute(query)


def show_pgtable(curs):
    # We can query the existence of tables themselves
    show_tables = """
    SELECT *
    FROM
    pg_catalog.pg_tables
    WHERE schemaname != 'pg_catalog'
    AND schemaname != 'information_schema';
    """

    curs.execute(show_tables)
    print("created tables in postgres:\n")
    print(curs.fetchall())

# Step 3 Insert into the table, exclude the sqlite identifier in 'result' as postgres generates its
def pg_insert(curs):
    example_insert = """
    INSERT INTO mtable
    (Survived, PClass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Abroad, Fare)
    VALUES """ + str(result[0][1:]) + ";"

    print("example insert:", example_insert)
    print("Inserting data into postgres ...")

    idx = 0
    for item in result:
        idx += 1
        insert_item = """
        INSERT INTO mtable
        (Survived, PClass, Name, Sex, Age, Siblings_Spouses_Aboard, Parents_Children_Abroad, Fare)
        VALUES """ + str(item[1:]) + ";"
        curs.execute(insert_item)

        # Print the inserted items on the go
        # curs.execute(f'SELECT * FROM mtable WHERE passenger_id = {idx};')
        # pg_item = curs.fetchall()
        # print(pg_item)
    print("Insertion complete")


def pg_sqlite_check(pg_curs, result):
    # Check every row of sqlite3 db vs postgres db for sanity
    pg_curs.execute('SELECT * FROM mtable;')
    pg_result = pg_curs.fetchall()
    print(pg_result[:2])

    # Iterating through two lists in parallel using zip()
    for item, pg_item in zip(result, pg_result):
        if item[1:] != pg_item[1:]:
            print(f'from sqlite:\n {item}\n different from postgres:\n{pg_item}')


if __name__ == '__main__':
    print("**********Main******************")
    # Create sqlite table from csv file
    result = csv_to_sqlite()

    # Connect to Postgress db
    pg_conn, pg_curs = refresh_connection_and_cursor()

    # List existing tables
    print("Print list of tables in Postgres:\n")
    print(pg_table_list(pg_curs))

    # Drop existing tables
    # table_list = ["mtable"]
    # pg_table_drop(pg_curs, table_list)
    # pg_conn.commit()

    # Create pg table
    # create_pgtable(pg_curs)

    # Show all pg tables
    show_pgtable(pg_curs)

    # Insert into pg table, Loading phase
    # pg_insert(pg_curs)
    # pg_conn.commit()

    #check postgress content vs sqlite
    pg_sqlite_check(pg_curs, result)

    # Table information:
    q_alltables = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES"
    q_columns = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = 'mtable';"
    pg_curs.execute(q_columns)
    print(f"column names: \n {pg_curs.fetchall()}")

    # Queries
    q_list=[]
    # https://api.elephantsql.com/console/7f73683a-ea33-48c2-97b2-40ef85c6c35f/browser?
    # How many passengers survived, and how many died?
    q_list.append(("How many passengers survived?",
                   "SELECT COUNT(DISTINCT passenger_id) FROM mtable WHERE survived=1;"))
    q_list.append(("How many passengers died?",
                   "SELECT COUNT(DISTINCT passenger_id) FROM mtable WHERE survived=0;"))
    q_list.append(("How many passengers were in each class?",
                   "SELECT COUNT(pclass) FROM mtable GROUP BY pclass ORDER BY pclass ASC;"))
    q_list.append(("How many passengers survived within each class?",
                   "SELECT COUNT(DISTINCT passenger_id) FROM mtable WHERE survived=1 GROUP BY pclass ORDER BY pclass ASC"))
    q_list.append(("How many passengers died within each class?",
                   "SELECT COUNT(DISTINCT passenger_id) FROM mtable WHERE survived=0 GROUP BY pclass ORDER BY pclass ASC"))
    q_list.append(("What was the average age of survivors?",
                   "SELECT AVG(age) FROM mtable WHERE survived=1"))
    q_list.append(("What was the average age of nonsurvivors?",
                   "SELECT AVG(age) FROM mtable WHERE survived=0"))
    q_list.append(("What was the average age of survived vs nonsurvivors?",
                   "SELECT AVG(age)/(SELECT AVG(age) FROM mtable WHERE survived=1) FROM mtable WHERE survived=0"))
    q_list.append(("What was the average age of each passenger class?",
                   "SELECT AVG(age) FROM mtable GROUP BY pclass ORDER BY pclass ASC"))
    q_list.append(("What was the average fare by passenger class? By survival?",
                   "SELECT pclass, survived, AVG(age) FROM mtable GROUP BY pclass, survived ORDER BY pclass ASC, survived DESC"))
    q_list.append(("How many siblings/spouses aboard on average, by passenger class? By survival?",
                   ("SELECT pclass, survived, AVG(siblings_spouses_aboard) FROM mtable GROUP BY pclass, survived "
                    "ORDER BY pclass ASC, survived DESC")))
    q_list.append(("How many parents/children aboard on average, by passenger class? By survival?",
                   ("SELECT pclass, survived, AVG(parents_children_abroad) FROM mtable GROUP BY pclass, survived "
                    "ORDER BY pclass ASC, survived DESC;")))
    q_list.append(("Do any passengers have the same name?",
                   "SELECT name, COUNT(*) FROM mtable GROUP BY name HAVING COUNT(name) > 1"))

    for i, q in enumerate(q_list):
        pg_curs.execute(q[1])
        print(f'{q[0]}:\n{pg_curs.fetchall()}')


    # Closing out cursor/connection to wrap up
    pg_curs.close()
    pg_conn.close()
