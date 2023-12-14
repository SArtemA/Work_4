import json
import sqlite3

def parse_data(file_js):
    with open(file_js, "r", encoding="utf-8") as file:
        items = json.load(file)
        print(items)
    return items

def connect_to_db(file_db):
    connection = sqlite3.connect(file_db)
    connection.row_factory = sqlite3.Row
    return connection

def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT INTO table_1 (id, name, city, begin, system, tours_count, min_rating, time_on_game) 
        VALUES( :id, :name, :city, :begin, :system, :tours_count, :min_rating, :time_on_game
            )
        """, data)
    db.commit()

def filter_data(db, limit):
    cursor = db.cursor()

    result_1 = db.cursor().execute(f"""SELECT * 
                                    FROM table_1 
                                    ORDER BY time_on_game DESC 
                                    LIMIT {limit}""")
    items = []

    for row in result_1:
        item = dict()
        item["id"] = row[0]
        item["name"] = row[1]
        item["city"] = row[2]
        item["begin"] = row[3]
        item["system"] = row[4]
        item["tours_count"] = row[5]
        item["min_rating"] = row[6]
        item["time_on_game"] = row[7]
        items.append(item)

    db.commit()

    with open("filtered_data.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))


def describe_data(db):
    cursor = db.cursor()

    result_2 = db.cursor().execute("""SELECT SUM(tours_count) AS sum_tours_count,
                                            MIN(tours_count) AS min_tours_count,
                                            MAX(tours_count) AS max_tours_count,
                                            AVG(tours_count) AS avg_tours_count
                                    FROM table_1""")
    db.commit()

    print("сумма, минимум, максимум, среднее по  полю 'tours_count'")
    print(*result_2.fetchone())


def distinct_count(db):
    cursor = db.cursor()

    result_3 = db.cursor().execute("""SELECT city, COUNT(city)
                                    FROM table_1
                                    GROUP BY city""")

    db.commit()

    print("Частота встречаемости для категориального поля 'city'")
    for row in result_3:
        print(*row)

    return




def sorted_filter_data(db, limit):
    cursor = db.cursor()

    result_4 = db.cursor().execute("""SELECT * 
                                    FROM table_1 
                                    WHERE system == "Olympic"
                                    ORDER BY tours_count DESC 
                                    """)
    items = []

    for row in result_4:
        item = dict()
        item["id"] = row[0]
        item["name"] = row[1]
        item["city"] = row[2]
        item["begin"] = row[3]
        item["system"] = row[4]
        item["tours_count"] = row[5]
        item["min_rating"] = row[6]
        item["time_on_game"] = row[7]
        items.append(item)

    db.commit()

    with open("sorted_filter_table_task1.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))



VAR = 78
limit = VAR + 10

items = parse_data("task_1_var_78_item.json")

db = connect_to_db('base_1')

insert_data(db, items)

filter_data(db, limit)

describe_data(db)
print()
distinct_count(db)

sorted_filter_data(db, limit)