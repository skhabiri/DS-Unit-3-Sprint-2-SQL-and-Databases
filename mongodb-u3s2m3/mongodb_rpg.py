import os
import sqlite3
import pymongo
from dotenv import load_dotenv
from pathlib import Path

env_path = Path('.') / '.env'
load_dotenv(dotenv_path=env_path, verbose=True)

os.system(f'curl ipecho.net/plain')

password = os.getenv("password")
dbname = 'rpgdb'
connection = ('mongodb+srv://seandb:' + password + '@cluster0.8kgy0.mongodb.net/' + dbname +
              '?retryWrites=true&w=majority')
print("\n", connection)

# Create a client to the MongoDB instance.
client = pymongo.MongoClient(connection)

# reference to the rpgdb db through mongo client
# db = client.rpgdb
db = client['rpgdb']
# reference to 'character' collection in 'rpgdb'
col = db['character']

print("dir(db)\n", dir(db))
print("dir(col)\n", dir(col))

print("list of db's:\n", list(client.list_database_names()))
print("list of collections:\n", list(db.list_collection_names()))

# drop all databases
# client.drop_database('rpgdb')

# Delete the collections from rpgdb. if db is empty, it will be removed too
for collection in list(db.list_collection_names()):
    # delete the documents in collection
    x = db[collection].delete_many({})
    print(x.deleted_count, " documents deleted.")
    # drop the collection
    db[collection].drop()

# List the items in test collection
curs = col.find()
print(f'Items in db["character"]:\n {list(curs)}')


# SQLITE3 RPG DATABASE
url = "https://github.com/skhabiri/SQL-Databases-u3s2/blob/master/sqlite-u3s2m1/rpg_db.sqlite3?raw=true"
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
print("schema type:\n", type(rpg_schema))

# Adding 'doc_type' to be used as another column name
# Tuple of length 2 concatenated as a part of first row
# 
rpg_schema1 = [('doc_type',) * 2] + rpg_schema
# [('doc_type', 'doc_type'), (0, 'character_id', 'integer', 1, None, 1)]
print("rpg_schema1[:2]", rpg_schema1[:2])

# Adding 'rpg_character' to the records for later use in mongo
rpg_character1 = characters[:]
# comma , at the end of tuple says it's not a string but tuple
# tuple addition concatinates the tuples into a single tuple
# ading the table name as the first element of the tuple in each row
rpg_character1 = [(('rpg_character',) + item) for item in rpg_character1]
print("rpg_character1[:3]\n", rpg_character1[:3])

# Constructing a list of dictionaries where each row of charactercreator_character is
# converted to a dictionary with column names as keys and row values as values
rpg_list = []
rpg_list = [
            {rpg_schema1[i][1]: rpg_character1[j][i] for i in range(len(rpg_schema1))
             }
            for j in range(len(rpg_character1))
            ]
print("rpg_list[:3]\npython", rpg_list[:3])
print(len(rpg_list))
"""
[{'doc_type': 'rpg_character', 'character_id': 1, 'name': 'Aliquid iste optio reiciendi', 
'level': 0, 'exp': 0, 'hp': 10, 'strength': 1, 'intelligence': 1, 'dexterity': 1, 'wisdom': 1}, 
{'doc_type': 'rpg_character', 'character_id': 2, 'name': 'Optio dolorem ex a', 'level': 0, 
'exp': 0, 'hp': 10, 'strength': 1, 'intelligence': 1, 'dexterity': 1, 'wisdom': 1}, 
{'doc_type': 'rpg_character', 'character_id': 3, 'name': 'Minus c', 'level': 0, 'exp': 0, 
'hp': 10, 'strength': 1, 'intelligence': 1, 'dexterity': 1, 'wisdom': 1}]
"""

# Step 3 Loading MongodB with documents
col.insert_many(rpg_list)

#mongo content
curs = col.find()
print("mongodb content:\n", list(curs)[:3])

#Limit number of queries
print("filtered db:\n", list(col.find({'wisdom': 'rpg_character'}).limit(3)))

