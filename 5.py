import sqlite3
import pandas as pd
import csv
import json

conn = sqlite3.connect('cinema.db')
cursor = conn.cursor()

cursor.executescript("""
CREATE TABLE IF NOT EXISTS Films (
    film_id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    genre TEXT,
    duration INTEGER,
    rating REAL
);

CREATE TABLE IF NOT EXISTS Sessions (
    session_id INTEGER PRIMARY KEY AUTOINCREMENT,
    film_id INTEGER NOT NULL,
    date TEXT NOT NULL,
    time TEXT NOT NULL,
    hall INTEGER NOT NULL,
    FOREIGN KEY (film_id) REFERENCES Films(film_id)
);

CREATE TABLE IF NOT EXISTS Tickets (
    ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER NOT NULL,
    seat TEXT NOT NULL,
    price REAL NOT NULL,
    FOREIGN KEY (session_id) REFERENCES Sessions(session_id)
);
""")

conn.commit()

films_df = pd.read_csv('./resources/5/films.csv')
sessions_df = pd.read_csv('./resources/5/sessions.csv')
tickets_df = pd.read_csv('./resources/5/tickets.csv')

films_df.to_sql('Films', conn, if_exists='append', index=False)
sessions_df.to_sql('Sessions', conn, if_exists='append', index=False)
tickets_df.to_sql('Tickets', conn, if_exists='append', index=False)

conn.commit()

queries = [
    # Запрос 1: Выборка с простым условием + сортировка + ограничение
    "SELECT title, genre, rating FROM Films WHERE rating > 8.5 ORDER BY rating DESC LIMIT 2",
    # Запрос 2: Подсчет объектов по условию
    "SELECT COUNT(*) FROM Tickets WHERE price > 10",
    # Запрос 3: Группировка
    "SELECT film_id, COUNT(*) AS session_count FROM Sessions GROUP BY film_id",
    # Запрос 4:  Функция агрегации (среднее значение)
    "SELECT AVG(price) FROM Tickets",
    # Запрос 5: Обновление данных
    "UPDATE Films SET rating = 9.1 WHERE film_id = 1",
    # Запрос 6:  Выборка с JOIN
    "SELECT f.title, s.date, s.time FROM Films f JOIN Sessions s ON f.film_id = s.film_id WHERE f.genre = 'Action'",
    # Запрос 7: Подзапрос
    "SELECT title FROM Films WHERE film_id IN (SELECT film_id FROM Sessions WHERE date = '2024-03-08')"
]

results = []
for query in queries:
    cursor.execute(query)
    result = cursor.fetchall()
    results.append({"query": query, "result": [list(row) for row in result]})

conn.commit()
conn.close()

serializable_results = []
for item in results:
    new_item = {}
    new_item["query"] = item["query"]
    new_item["result"] = [list(row) for row in item["result"]]
    serializable_results.append(new_item)


with open('results.json', 'w', encoding='utf-8') as f:
    json.dump(serializable_results, f, indent=2, ensure_ascii=False)