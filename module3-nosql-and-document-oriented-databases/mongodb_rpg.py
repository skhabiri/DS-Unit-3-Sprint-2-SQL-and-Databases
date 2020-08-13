import os
import sqlite3
import pymongo

os.system(f'curl ipecho.net/plain')

password = 'xsYtRRehkUz29Hbw'  # Reset it if it leaks
dbname = 'test'
connection = ('mongodb+srv://seandb:' + password + '@cluster0.8kgy0.mongodb.net/' + dbname +
              '?retryWrites=true&w=majority')
print("\n", connection)
client = pymongo.MongoClient(connection)

db = client.test

print(dir(db))
print(dir(db.test))

# Delete the collection test from database db
x = db["test"].delete_many({})
print(x.deleted_count, " documents deleted.")

# List the items in test collection
curs = db.test.find()
print(f'Items in the db["test"]:\n {list(curs)}')

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

# schema for sqlite3
sl1_curs.execute('PRAGMA table_info(charactercreator_character);')
rpg_schema = sl1_curs.fetchall()
print(f'Schema for RPG Characters is \n{rpg_schema}')

# Adding 'doc_type' to schema for later use
# Tuple of length 2 concatenated as the first row
rpg_schema1 = [('doc_type',) * 2] + rpg_schema
print(rpg_schema1[:2])

# Adding 'rpg_character' to the records for later use in mongo
# concatinating tuples inside a list
rpg_character1 = characters[:]
# , at the end of tuple says it's not a string but tuple
rpg_character1 = [(('rpg_character',) + item) for item in rpg_character1]
print(rpg_character1[:3])

# Construction A list of dictionary documents
rpg_list = []
rpg_list = [
            {rpg_schema1[i][1]: rpg_character1[j][i] for i in range(len(rpg_schema1))
             }
            for j in range(len(rpg_character1))
            ]
print(len(rpg_list))
# print(rpg_list[:3])

# Step 3 Loading MongodB with documents
db["test"].insert_many(rpg_list)

#mongo content
curs = db.test.find()
print("mongodb content:\n", list(curs)[:3])

#Limit number of queries
list(db.test.find({'doc_type': 'rpg_character'}).limit(3))

