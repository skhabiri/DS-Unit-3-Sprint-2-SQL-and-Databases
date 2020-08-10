import sqlite3

conn = sqlite3.connect("rpg_db.sqlite3")
curs = conn.cursor()


def execute_query(cursor, query):
    cursor.execute(query)
    return cursor.fetchall()


if __name__ == '__main__':

    Total_char = """
    SELECT COUNT(character_id)
    FROM charactercreator_character
    """
    result = execute_query(curs, Total_char)
    print("Total number of characters:")
    print(result)

    char_class = ["mage", "thief", "cleric", "fighter"]
    results = []

    for idx, item in enumerate(char_class):
        query = """
        SELECT COUNT(character_ptr_id)
        FROM charactercreator_""" + item
        results.append(execute_query(curs, query))

        print(f"{item} characters:")
        print(results[idx][0][0])
