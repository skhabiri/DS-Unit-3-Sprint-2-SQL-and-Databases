import sqlite3

c = None
conn = None


# Defining a function to refresh connection and cursor
def refresh_cc(conn=conn, c=c):
    try:
        c.close()
        conn.close()
    except: pass
    conn = sqlite3.connect('study_part1.sqlite3')
    c = conn.cursor()
    return conn, c


# Table names in sqlite3
def tab_names(c):
    names = []
    res = c.execute("SELECT name FROM sqlite_master WHERE type='table';")
    for name in res:
        names.append(name[0])

    # sqlite3 schema (INTEGER, TEXT, REAL)
    # print("sqlite schema:\n")
    # print(curs.execute('PRAGMA table_info(mtable);').fetchall())

    return names


def insertData(table, columns, values):
    """ table is a string for table name
    columns is string and values is tuple (no quote)
    table = school_tab
    columns = (student, studied, grade, age, sex)
    values = ('Lion-O', 'True', 85, 24, 'Male')
    """
    query = 'INSERT INTO ' + table + ' ' + columns + f'VALUES {values};'
    return query


# Initialization
conn, c = refresh_cc()
names = tab_names(c)
for name in names:
    c.execute(f"DROP TABLE {name}")
conn.commit()


createTable = """CREATE TABLE school_tab(
                                        student TEXT, 
                                        studied TEXT, 
                                        grade INT, 
                                        age INT, 
                                        sex TEXT
                                        );"""

c.execute(createTable)

values=[('Lion-O', 'True', 85, 24, 'Male'),
        ('Cheetara', 'True', 95, 22, 'Female'),
        ('Mumm-Ra', 'False', 65, 153, 'Male'),
        ('Snarf', 'False', 70, 15, 'Male'),
        ('Panthro', 'True', 80, 30, 'Male')]

columns = "(student, studied, grade, age, sex)"
for value in values:
    q = insertData("school_tab", columns, value)
    c.execute(q)

qfetch = "SELECT * FROM school_tab;"
data = c.execute("SELECT * FROM school_tab;").fetchall()
print(data)


