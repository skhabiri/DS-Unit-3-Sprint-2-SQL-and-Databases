import pandas as pd
# import numpy as np
import sqlite3

data = pd.read_csv("./buddymove_holidayiq.csv")
print(data.head())
print(data.shape)
print(f'Number of Nulls: {data.isna().sum(axis=0).sum()}')

conn = sqlite3.connect("buddymove_holidayiq.sqlite3")
curs = conn.cursor()
data.to_sql(name="review", con=conn, if_exists='replace')

# Total number of reviews
query = """SELECT COUNT("User Id") FROM review"""
curs.execute(query)
result = curs.fetchall()
print(f'number of rows: {result}')

# more than 100 review for nature and shopping
query = """SELECT COUNT("USER Id") FROM review
WHERE "Nature" >= 100 AND "Shopping" >= 100
"""
curs.execute(query)
result = curs.fetchall()
print(f'Number of review for nature an shopping above 100: {result[0][0]}')

# Average number of reviews in each category
print(data.columns[1:])
for item in list(data.columns[1:]):
    print("average of ", item, "reviews:")
    query = "SELECT AVG(" + item + ") FROM review"
    curs.execute(query)
    result = curs.fetchall()
    print(round(result[0][0], 2))