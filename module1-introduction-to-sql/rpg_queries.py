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

    # Number of characters in each class
    char_class = ["mage", "thief", "cleric", "fighter"]
    results = []

    for idx, item in enumerate(char_class):
        query = """
        SELECT COUNT(character_ptr_id)
        FROM charactercreator_""" + item
        results.append(execute_query(curs, query))

        print(f"{item} characters:")
        print(results[idx][0][0])

    # How many total Items?
    query = """
    SELECT COUNT(item_id)
    FROM armory_item
    """
    print("Total items:")
    print(execute_query(curs, query))

    #How many of the Items are weapons? How many are not?
    query = """
    SELECT COUNT(item_ptr_id)
    FROM armory_weapon
    """
    print("Total Weapons:")
    print(execute_query(curs, query))

    #How many of the Items are not weapons?
    query="""
    SELECT COUNT(*) FROM
    (SELECT item_id  FROM armory_item WHERE item_id NOT IN  (SELECT item_ptr_id FROM armory_weapon));
    """
    query_alternative = """SELECT COUNT(*) FROM armory_item AS ai
                            LEFT JOIN armory_weapon AS aw
                            ON ai.item_id=aw.item_ptr_id 
                            WHERE aw.item_ptr_id IS Null
                            """
    print("Non Weapon items:")
    print(execute_query(curs, query))
    print(execute_query(curs, query_alternative))

    #How many of the Items are not weapons? alternative way
    query="""
    SELECT COUNT(item_id) FROM
    (SELECT item_id FROM armory_item LEFT JOIN armory_weapon ON item_id=item_ptr_id WHERE item_ptr_id IS NULL)
    """
    print("Non Weapon items: alternative")
    print(execute_query(curs, query))

    #How many Items does each character have? (Return first 20 rows)
    query="""
    SELECT character_id, COUNT(item_id)
    FROM charactercreator_character_inventory
    GROUP BY 1 ORDER BY 2 DESC LIMIT 20
    """
    print("ITEMS PER CHARACTER:")
    print(execute_query(curs, query))

    #How many Weapons does each character have? (Return first 20 rows)
    query="""
    SELECT character_id, COUNT(item_id) FROM
    (SELECT character_id, item_id, item_ptr_id FROM charactercreator_character_inventory, armory_weapon
    WHERE item_id=item_ptr_id)
    GROUP BY character_id ORDER BY 2 DESC LIMIT 20
    """
    query_alternative=""" SELECT character_id, COUNT(item_id) 
    FROM charactercreator_character_inventory 
    INNER JOIN armory_weapon 
    ON item_id=item_ptr_id 
    GROUP BY character_id 
    ORDER BY COUNT(item_id) DESC LIMIT 20
    """

    print("WEAPONS PER CHARACTER:")
    print(execute_query(curs, query))
    print(execute_query(curs, query_alternative))


    # On average, how many Items does each Character have?
    query="""
    SELECT AVG(item_count) FROM
    (SELECT character_id, COUNT(item_id) AS item_count
    FROM charactercreator_character_inventory
    GROUP BY 1 ORDER BY 2 DESC)
    """
    print("Average number of items per character:")
    print(execute_query(curs, query))

    # On average, how many Weapons does each character have?
    query="""
    SELECT AVG(weapon_count) FROM 
    (SELECT character_id, COUNT(item_id) AS weapon_count FROM 
    (SELECT character_id, item_id, item_ptr_id FROM charactercreator_character_inventory, armory_weapon 
    WHERE item_id=item_ptr_id) 
    GROUP BY character_id)
    """

    query_alternative = """SELECT AVG(wp_count) FROM
    (SELECT COUNT(item_id) AS wp_count FROM charactercreator_character_inventory
    INNER JOIN armory_weapon
    ON item_id = item_ptr_id
    GROUP BY character_id)
    """
    print("Average number of weapons per character:")
    print(execute_query(curs, query))
    print(execute_query(curs, query_alternative))