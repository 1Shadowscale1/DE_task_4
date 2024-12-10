import sqlite3
import pickle
import json

# Загрузка данных из файла .pkl
with open('./resources/1-2/subitem.pkl', 'rb') as f:
    subitem_data = pickle.load(f)


# Создание таблицы subitems и связь с items. Скрипт предполагает, что уже есть база данных item_db.db из первого задания
conn = sqlite3.connect('item_db.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS subitems (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        comment TEXT,
        convenience INTEGER,
        functionality INTEGER,
        rating REAL,
        security INTEGER,
        FOREIGN KEY (name) REFERENCES items(name)
    )
''')
conn.commit()


# Запись данных в таблицу subitems
for item in subitem_data:
    cursor.execute("INSERT INTO subitems (name, comment, convenience, functionality, rating, security) VALUES (?, ?, ?, ?, ?, ?)",
                   (item['name'], item['comment'], item['convenience'], item['functionality'], item['rating'], item['security']))
conn.commit()


# Запросы с использованием связи между таблицами

# Запрос 1:  Информация о зданиях с рейтингом выше 3 и их адреса
cursor.execute("""
    SELECT 
        subitems.name, 
        subitems.rating, 
        items.street, 
        items.city
    FROM 
        subitems
    INNER JOIN 
        items ON subitems.name = items.name
    WHERE 
        subitems.rating > 3
    LIMIT 10
""")
results = cursor.fetchall()
print("Запрос 1:")
for row in results:
    print(row)

# Запрос 2: Средний рейтинг удобства (convenience) для каждого города.
cursor.execute("""
    SELECT 
        items.city, 
        AVG(subitems.convenience) AS avg_convenience
    FROM 
        subitems
    INNER JOIN 
        items ON subitems.name = items.name
    GROUP BY 
        items.city
    LIMIT 10
""")
results = cursor.fetchall()
print("\nЗапрос 2:")
for row in results:
    print(row)


# Запрос 3: Количество зданий с уровнем безопасности (security) выше 4 в каждом городе.
cursor.execute("""
    SELECT 
        items.city, 
        COUNT(subitems.name) AS count_high_security
    FROM 
        subitems
    INNER JOIN 
        items ON subitems.name = items.name
    WHERE 
        subitems.security > 4
    GROUP BY 
        items.city
    LIMIT 10
""")
results = cursor.fetchall()
print("\nЗапрос 3:")
for row in results:
    print(row)


conn.close()