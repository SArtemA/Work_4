import json
import sqlite3



def load_data(file_txt):
    items = []
    with open(file_txt, 'r', encoding='utf-8') as file:
        data = file.readlines()
        item = dict()
        for i in data:
            if i == '=====\n':
                items.append(item)
                item = dict()
            else:
                i = i.strip()
                splitted = i.split('::')
                if splitted[0] == 'price':
                    item[splitted[0]] = int(splitted[1])
                else:
                    item[splitted[0]] = splitted[1]

    return items


def connect_to_db(file_db):
    connection = sqlite3.connect(file_db)
    connection.row_factory = sqlite3.Row

    return connection


def insert_data(db, data):
    cursor = db.cursor()
    cursor.executemany("""
        INSERT or REPLACE INTO table_2 (_id, name, place, prise)
        VALUES(
        (SELECT id FROM table_1 WHERE name = :name), :name, :place, :prise)
        """, data)

    db.commit()


def first_query(db, name):

    cursor = db.cursor()

    result_1 = cursor.execute("""
                                SELECT prise 
                                FROM table_2 
                                WHERE name = (SELECT name FROM table_1 WHERE name = ?)
                                ORDER BY prise DESC
                            """, [name])
    items = []

    for row in result_1.fetchall():
        item = dict(row)
        print(item)
        items.append(item)

    with open(f'first_query_task2.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))
    cursor.close()

def second_query(db, name):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
        MAX(time_on_game) as time_on_game
        FROM table_1
        WHERE id = (SELECT _id FROM table_2 WHERE prise < ?)
        """, [name])
    items = []
    for row in result.fetchall():
        item = dict(row)
        print(item)
        items.append(item)

    with open(f'second_query_task_2.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))
    cursor.close()

def third_query(db):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT 
        name, prise
        FROM table_2
        WHERE _id = (SELECT id FROM table_1 WHERE tours_count > 10)
        LIMIT 10
        """)
    items = []
    for row in result.fetchall():
        item = dict(row)
        print(item)
        items.append(item)

    with open(f'third_query_task_2.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))
    cursor.close()

items = load_data("task_2_var_78_subitem.text")
db = connect_to_db("base_1")
insert_data(db, items)
first_query(db, 'Пальма 1962')
second_query(db, '3500')
third_query(db)
