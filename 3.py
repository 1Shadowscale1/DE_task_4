import sqlite3
import json
import random

conn = sqlite3.connect('music_db.db')
cursor = conn.cursor()

cursor.execute("""
    CREATE TABLE tracks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        artist TEXT NOT NULL,
        song TEXT NOT NULL,
        duration_ms INTEGER,
        year INTEGER,
        tempo REAL,
        genre TEXT,
        explicit BOOLEAN,
        loudness REAL,
        popularity INTEGER,
        danceability REAL
    )
    """)

# Импорт из part_1.text
with open('./resources/1-2/_part_1.text', 'r') as f:
    track_data = []
    current_track = {}
    for line in f:
        line = line.strip()
        if line == '=====':
            track_data.append(current_track)
            current_track = {}
        elif ':' in line:
            key, value = line.split('::')
            key = key.strip()
            value = value.strip()
            try:
                if '.' in value:
                    value = float(value)
                else:
                    value = int(value) if value.isdigit() else value.lower() == "true"
            except ValueError:
                pass
            current_track[key] = value


    for track in track_data:
        cursor.execute("INSERT INTO tracks (artist, song, duration_ms, year, tempo, genre, explicit, loudness) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                       (track['artist'], track['song'], track['duration_ms'], track['year'], track['tempo'], track['genre'], track['explicit'], track['loudness']))

# Импорт из part_2.json
with open('./resources/1-2/_part_2.json', 'r') as f:
    data = json.load(f)
    for track in data:
        cursor.execute("INSERT INTO tracks (artist, song, duration_ms, year, tempo, genre, explicit, popularity, danceability) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                       (track['artist'], track['song'], int(track['duration_ms']), int(track['year']), float(track['tempo']), track['genre'], track['explicit'] == 'True', int(track['popularity']), float(track['danceability'])))

conn.commit()
conn.close()

conn = sqlite3.connect('music_db.db')
cursor = conn.cursor()

# Запрос 1: первые 14 отсортированных по duration_ms строк в JSON
cursor.execute("SELECT * FROM tracks ORDER BY duration_ms LIMIT 14")
results = cursor.fetchall()
json_output = []
for row in results:
  json_output.append(dict(zip([column[0] for column in cursor.description], row)))
with open('query1.json', 'w') as f:
    json.dump(json_output, f, indent=4)


# Запрос 2: сумма, мин, макс, среднее duration_ms
cursor.execute("SELECT SUM(duration_ms), MIN(duration_ms), MAX(duration_ms), AVG(duration_ms) FROM tracks")
results = cursor.fetchone()
print("Сумма duration_ms:", results[0])
print("Мин duration_ms:", results[1])
print("Макс duration_ms:", results[2])
print("Среднее duration_ms:", results[3])

# Запрос 3: частота встречаемости genre
cursor.execute("SELECT genre, COUNT(*) FROM tracks GROUP BY genre")
results = cursor.fetchall()
print("\nЧастота встречаемости genre:")
for genre, count in results:
    print(f"{genre}: {count}")

# Запрос 4: первые 19 отфильтрованных (year > 2015) отсортированных по tempo строк в JSON
cursor.execute("SELECT * FROM tracks WHERE year > 2015 ORDER BY tempo LIMIT 19")
results = cursor.fetchall()
json_output = []
for row in results:
  json_output.append(dict(zip([column[0] for column in cursor.description], row)))

with open('query4.json', 'w') as f:
    json.dump(json_output, f, indent=4)


conn.close()