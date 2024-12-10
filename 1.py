import sqlite3
import json

# Создание таблицы
conn = sqlite3.connect('item_db.db')
cursor = conn.cursor()
cursor.execute('''
    CREATE TABLE IF NOT EXISTS items (
        id INTEGER PRIMARY KEY,
        name TEXT,
        street TEXT,
        city TEXT,
        zipcode INTEGER,
        floors INTEGER,
        year INTEGER,
        parking BOOLEAN,
        prob_price INTEGER,
        views INTEGER
    )
''')
conn.commit()

# Чтение данных из файла и запись в базу данных
with open('./resources/1-2/item.text', 'r') as f:
    item_data = []
    current_item = {}
    for line in f:
        line = line.strip()
        if line == '=====':
            item_data.append(current_item)
            current_item = {}
        elif '::' in line:
            key, value = line.split('::')
            key = key.strip()
            value = value.strip()
            try:
                if '.' in value:
                    value = float(value)
                elif value.lower() == 'true':
                    value = True
                elif value.lower() == 'false':
                    value = False
                else:
                    value = int(value)
            except ValueError:
                pass  # Пропускаем ошибки преобразования
            current_item[key] = value

    for item in item_data:
        cursor.execute("INSERT INTO items (id, name, street, city, zipcode, floors, year, parking, prob_price, views) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (item['id'], item['name'], item['street'], item['city'], item['zipcode'], item['floors'], item['year'], item['parking'], item['prob_price'], item['views']))

conn.commit()


# Запросы к базе данных

# Запрос 1: первые 14 отсортированных по prob_price строк в JSON
cursor.execute("SELECT * FROM items ORDER BY prob_price LIMIT 14")
results = cursor.fetchall()
json_output = []
for row in results:
    json_output.append(dict(zip([column[0] for column in cursor.description], row)))
with open('query1.json', 'w') as f:
    json.dump(json_output, f, indent=4)

# Запрос 2: сумма, мин, макс, среднее prob_price
cursor.execute("SELECT SUM(prob_price), MIN(prob_price), MAX(prob_price), AVG(prob_price) FROM items")
results = cursor.fetchone()
print("Сумма prob_price:", results[0])
print("Мин prob_price:", results[1])
print("Макс prob_price:", results[2])
print("Среднее prob_price:", results[3])

# Запрос 3: частота встречаемости city
cursor.execute("SELECT city, COUNT(*) FROM items GROUP BY city")
results = cursor.fetchall()
print("\nЧастота встречаемости city:")
for city, count in results:
    print(f"{city}: {count}")

# Запрос 4: первые 14 отфильтрованных (year > 1800) отсортированных по views строк в JSON
cursor.execute("SELECT * FROM items WHERE year > 1800 ORDER BY views LIMIT 14")
results = cursor.fetchall()
json_output = []
for row in results:
    json_output.append(dict(zip([column[0] for column in cursor.description], row)))
with open('query4.json', 'w') as f:
    json.dump(json_output, f, indent=4)

conn.close()