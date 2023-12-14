import json
import msgpack
import  sqlite3


def open_msgpack(file_msg):
    with open(file_msg, 'rb') as file:
        res = msgpack.load(file)

    return  res


def open_js(file_js):
    with open(file_js, "r", encoding="utf-8") as file:
        items = json.load(file)

    return items


def connect_to_db(file_db):
    return sqlite3.connect(file_db)


def insert_data(db, data):
    cursor = db.cursor()

    cursor.executemany("""
        INSERT INTO table_3 (artist, song, duration_ms, year, tempo, genre) 
        VALUES(
        :artist, :song, :duration_ms, :year, :tempo, :genre
            )
        """, data)
    db.commit()


def sorted_data(db, limit):
    cursor = db.cursor()

    result_1 = cursor.execute("""SELECT * 
                                FROM table_3 
                                ORDER BY year DESC 
                                LIMIT ?""", [limit])

    items = []
    for row in result_1:
        item = dict()
        item["artist"] = row[0]
        item["song"] = row[1]
        item["duration_ms"] = row[2]
        item["year"] = row[3]
        item["tempo"] = row[4]
        item["genre"] = row[5]
        items.append(item)

    cursor.close()

    with open("sorted_table.json", "w") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    return


def describe_data(db):
    cursor = db.cursor()

    result_2 = cursor.execute("""SELECT SUM(tempo) AS sum_tempo,
                                            MIN(tempo) AS min_tempo,
                                            MAX(tempo) AS max_tempo,
                                            AVG(tempo) AS avg_tempo
                                    FROM table_3""")

    print("Сумма, минимум, максимум, среднеее по 'tempo'")
    describe = result_2.fetchall()[0]
    item = dict({ "sum_tempo": describe[0],
                  "min_tempo": describe[1],
                  "max_tempo": describe[2],
                  "avg_tempo": describe[3]
                })

    print(item)

    cursor.close()


def count_caregori_data(db):
    cursor = db.cursor()

    result_3 = cursor.execute("""SELECT artist, COUNT(year) as count_artist
                                    FROM table_3
                                    GROUP BY  artist
                                    ORDER BY 2 DESC
                                    """)

    items = [dict(result_3.fetchall())]
    print(items)

    with open("filter_table.json", "w", encoding="utf-8") as file_json:
        file_json.write(json.dumps(items, ensure_ascii=False))

    cursor.close()
    return

def filt_num_pole(db,min_tempo, limit):
    cursor = db.cursor()
    result = cursor.execute("""
        SELECT * 
        FROM table_3 
        WHERE tempo > ?
        ORDER BY year DESC 
        LIMIT ?
        """, [min_tempo, limit])
    items = []
    for row in result:
        item = dict()
        item["artist"] = row[0]
        item["song"] = row[1]
        item["duration_ms"] = row[2]
        item["year"] = row[3]
        item["tempo"] = row[4]
        item["genre"] = row[5]
        items.append(item)
    cursor.close()
    print(items)
    with open(f'filt_num_pole.json', 'w', encoding='utf-8') as file:
        file.write(json.dumps(items, ensure_ascii=False))

VAR = 78
limit_1 = VAR + 10
limit_2 = VAR + 15

result = open_msgpack("task_3_var_78_part_1.msgpack") + open_js("task_3_var_78_part_2.json")

db = connect_to_db("base_3")
insert_data(db, result)
sorted_data(db, limit_1)
describe_data(db)
count_caregori_data(db)
filt_num_pole(db,111,limit_2)