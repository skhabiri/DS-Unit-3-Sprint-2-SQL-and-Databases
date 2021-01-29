# NoSQL and Document-oriented databases

NoSQL, no worries? Not exactly, but it's still a powerful approach for some
problems.

## Objectives

- Identify appropriate use cases for document-oriented databases
- Deploy and use a simple MongoDB instance

## Preparation

Sign up for an account at [MongoDB Atlas](https://www.mongodb.com/cloud/atlas),
the official hosted service of MongoDB with a generous (500mb) free tier. You
can also explore the many [MongoDB tools](http://mongodb-tools.com/) out there.

## To do

Another database, same data? Let's try to store the RPG data in our MongoDB
instance, and learn about the advantages and disadvantages of the NoSQL paradigm
in the process. We will depend on
[PyMongo](https://api.mongodb.com/python/current/) to connect to the database.

Note - the
[JSON](https://github.com/LambdaSchool/Django-RPG/blob/master/testdata.json)
representation of the data is likely to be particularly useful for this purpose.


## Resources and Extra

Put Titanic data in Big Data! That is, try to load `titanic.csv` from yesterday
into your MongoDB cluster.

Push MongoDB - it is flexible and can support fast iteration. 
____

# Module3: NOSQL-Document Oriented DB, MongoDB:
MongoDB does not have any schema and store any document as long as it is a valid json document. It’s been implemented based on map reduce paradigm which gives the advantage of dividing the tasks and distributing them to several workers, This allows to work with BigData and scaling while being fast. SImilar to SQL, Mongo supports CRUD operation. In Mongo, collections are similar to tables in relational databases. A created instance can contain multiple databases, and each database has multiple collections. Databases and collections in mongo are implicit. Meaning they are created when they are first used. Steps to follow:
* Create an account in https://www.mongodb.com/cloud/atlas .
* Create a cluster. The free plan gives 500MB of storage
* Connect to the cluster
   * It asks to add the ip address of the client to the white list. In colab `!curl ipecho.net/plain`
   * select driver as python and use full driver connection example to connect the client to the mongodb through pymongo.
* pipenv install pymongo; import pymongo or install dnspython
* for connecting if python version 3.6 didn’t work use 3.4 instead
   * client = pymongo.MongoClient(... ; db = client.rpgdb
* db.character is a collection that has several methods:
   *  db.character.insert_one({‘x’: 1})        # insert a  
   *  db.character.count_documents({‘x’: 1}) # count of all matching docs
   * db.character.find_one({‘x’: 1}) # returns the object id of the matching doc
   * curs = db.character.find({‘x’: 1}) # create a cursor object
   * list(curs)
* doc1 = {‘animal’: ‘whale’, ‘color’: ‘blue’, ‘number’: 7}; doc2 = {‘animal’: ‘panther’, ‘color’: ‘blue’, ‘number’: 2}; all_docs = [doc1, doc2]
* db.character.insert_many(all_docs)        # insert all docs
* list(db.character.find())        # list everything
* more_docs = []; for i in range(10): doc = {“even”: i%2==0}; doc[“value”]=i; more_docs.append(doc)
* db.character.insert_many(more_docs)
* list(db.character.find({“even”: True}))
* db.character.delete_many({“even”:False})
* list(db.character.find())
Let’s try to datapipeline the rpg database to Mongo. For Mongo we need the data type to be json, not tuple.
* rpg_character = (1, ‘king bob’, 10, 3, 0, 0, 0)
* rpg_doc = {‘sql_key’: rpg_charcater[0], ‘name’: rpg_character[1], ‘hp’:rpg_charactaer[2], ‘level’: rpg_character[3]}
* db.character.insert_one(rpg_doc)
* …
Libraries:
```
* import os
* import sqlite3
* import pymongo
* from dotenv import load_dotenv
* from pathlib import Path
```
https://github.com/skhabiri/SQL-Databases-u3s2/tree/master/mongodb-u3s2m3

