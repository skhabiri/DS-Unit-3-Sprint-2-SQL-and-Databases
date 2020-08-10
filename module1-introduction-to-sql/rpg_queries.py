import sqlite3

conn = sqlite3.connect("rpg_db.sqlite3")
curs = conn.cursor()
curs.execute(query)
curs.fetchall()


SELECT COUNT(character_id)
FROM charactercreator_character

if __name__ == '__main__':
    Total_char = """
    SELECT COUNT(character_id)
    FROM charactercreator_character
    """
    curs.execute(Total_char)
    results = curs.fetchall()
    print("Total number of characters:")
    print(results)

    
