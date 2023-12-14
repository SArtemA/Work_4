import json

import  sqlite3
import csv


def open_csv(file_name):
    with open(file_name, newline='\n', encoding='utf-8') as file:
        csv_reader = csv.reader(file, delimiter=';')
        items_0 = [row_0 for row_0 in csv_reader]
        items = []

        items_0.pop(0)
        for row in items_0:
            item = dict()
            item['name'] = row[0]
            item['price'] = row[1]
            item['quantity'] = row[2]
            if len(row) == 6:
                item['category'] = "no"
                item['fromCity'] = row[3]
                item['isAvailable'] = row[4]
                item['views'] = row[5]
            elif len(row) == 7:
                item['category'] = row[3]
                item['fromCity'] = row[4]
                item['isAvailable'] = row[5]
                item['views'] = row[6]
            else: print("ban")

            items.append(item)

    return items


def open_js(file_js):
    with open(file_js, "r", encoding="utf-8") as file:
        items = json.load(file)

    return items


def connect_to_db(file_db):
    return sqlite3.connect(file_db)


def insert_data(db, data):
    cursor = db.cursor()

    result = cursor.executemany("""
                                    INSERT INTO table_4 (name, price, quantity, category, fromCity, isAvailable, views) 
                                    VALUES (:name, :price, :quantity, :category, :fromCity, :isAvailable, :views)
                                    """, data)

    db.commit()

#updates
def handle_method(cursor, name, method, param=None):
    if method == 'remove':
        cursor.execute('DELETE FROM table_4 WHERE name = ?', [name])
    elif method == 'quantity_add':
        cursor.execute('UPDATE table_4 SET quantity = quantity + ?, version = version + 1 WHERE name = ?', [abs(param), name])
    elif method == 'quantity_sub':
        cursor.execute('UPDATE table_4 SET quantity = quantity - ?, version = version + 1 WHERE name = ? AND ((quantity - ?) > 0)', [abs(param), name, abs(param)])
    elif method == 'price_percent':
        cursor.execute('UPDATE table_4 SET price = ROUND(price * (1 + ?), 2), version = version + 1 WHERE name = ?', [param, name])
    elif method == 'price_abs':
        cursor.execute(f"UPDATE table_4 SET price = price + ?, version = version + 1 WHERE name = ? AND ((price + ?) > 0)", [param, name, param])
    elif method == 'available':
        cursor.execute('UPDATE table_4 SET isAvailable = ?, version = version + 1 WHERE name == ?', [1 if param else 0, name])
    else:
        raise ValueError(f'{method} метода нет!')

def handle_updates(db, data):
    cursor = db.cursor()
    for update in data:
        handle_method(db, update['name'], update['method'], update['param'])
    db.commit()

#	вывести топ-10 самых обновляемых товаров
def top_10(db):
    cursor = db.cursor()
    result = cursor.execute("""
    SELECT name, price, quantity, fromCity, isAvailable, views, version 
    FROM table_4 
    ORDER BY version DESC LIMIT 10""")
    items = []
    for row in result.fetchall():
        item = dict()
        item['name'] = row[0]
        item['price'] = row[1]
        item['quantity'] = row[2]
        item['category'] = row[3]
        item['fromCity'] = row[4]
        item['isAvailable'] = row[5]
        item['views'] = row[6]
        items.append(item)

    db.commit()

    return items

#	проанализировать цены товаров, найдя (сумму, мин, макс, среднее) для каждой группы, а также количество товаров в группе
def analises_price_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT fromCity,
                                        SUM(price) as sum_price,
                                        MIN(price) as min_price,
                                        MAX(price) as max_price,
                                        AVG(price) as avg_price,
                                        SUM(quantity) as all_quantity
                                FROM table_4
                                GROUP BY fromCity
                                ORDER BY all_quantity DESC""")
    items = []
    for row in result.fetchall():
        item = dict()
        item['fromCity'] = row[0]
        item['sum_price'] = row[1]
        item['min_price'] = row[2]
        item['max_price'] = row[3]
        item['avg_price'] = row[4]
        item['all_quantity'] = row[5]
        items.append(item)


    db.commit()

    return items


# 	проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров	проанализировать остатки товаров, найдя (сумму, мин, макс, среднее) для каждой группы товаров
def analises_quantity_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT category,
                                        SUM(quantity) as sum_quantity,
                                        MIN(quantity) as min_quantity,
                                        MAX(quantity) as max_quantity,
                                        AVG(quantity) as avg_quantity
                                FROM table_4
                                GROUP BY category
                                ORDER BY sum_quantity DESC""")
    items = []
    for row in result.fetchall():
        item = dict()
        item['category'] = row[0]
        item['sum_quantity'] = row[1]
        item['min_quantity'] = row[2]
        item['max_quantity'] = row[3]
        item['avg_quantity'] = row[4]
        items.append(item)
    db.commit()
    return items

# Найти товары, категории фрукт
def free_analises_products(db):
    cursor = db.cursor()
    result = cursor.execute("""SELECT name, price, quantity, category, fromCity, isAvailable, views, version
                                FROM table_4
                                WHERE category = 'fruit'
                                ORDER BY price DESC""")

    items = []
    for row in result.fetchall():
        item = dict()
        item['name'] = row[0]
        item['price'] = row[1]
        item['quantity'] = row[2]
        item['category'] = row[3]
        item['fromCity'] = row[4]
        item['isAvailable'] = row[5]
        item['views'] = row[6]
        item['version'] = row[7]
        items.append(item)
    db.commit()

    return items

def write_json(name, data):
    with open(name, "w", encoding="utf-8") as file:
        file.write(json.dumps(data, ensure_ascii=False))

data = open_csv("task_4_var_78_product_data.csv")
db = connect_to_db("base_4")
insert_data(db, data)
products_changes = open_js("task_4_var_78_update_data.json")
handle_updates(db, products_changes)



print()
print("вывести топ-10 самых обновляемых товаров")
for i in top_10(db): print(i)
write_json("top_10.json", top_10(db))

print()
print("сумма, мин, макс, среднее для каждой группы, количество товаров в группе")
for i in analises_price_products(db): print(i)
write_json("analises_price_products.json", analises_price_products(db))

print()
print("суммa, мин, макс, среднее для каждой группы товаров")
for i in analises_quantity_products(db):print(i)
write_json("analises_quantity_products.json", analises_quantity_products(db))

print()
print("Найти товары, количество которых болше 700")
for i in free_analises_products(db):print(i)
write_json("free_analises_products.json", free_analises_products(db))
