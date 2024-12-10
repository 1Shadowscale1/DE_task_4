import sqlite3
import pandas as pd
import msgpack


def load_data(msgpack_file, text_file):
    with open(msgpack_file, 'rb') as f:
        product_data = msgpack.unpackb(f.read(), raw=False)
    with open(text_file, 'r') as f:
        updates = f.readlines()
    return product_data, updates

def create_table(conn, cursor):
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS products (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            price REAL DEFAULT 0,
            quantity INTEGER DEFAULT 0,
            category TEXT,
            fromCity TEXT,
            isAvailable BOOLEAN DEFAULT 1,
            views INTEGER DEFAULT 0,
            update_count INTEGER DEFAULT 0
        );
    ''')
    conn.commit()

def insert_data(conn, cursor, product_data):
    for product in product_data:
        cursor.execute('''
            INSERT OR IGNORE INTO products (name, price, quantity, category, fromCity, isAvailable, views) 
            VALUES (?, ?, ?, ?, ?, ?, ?);
        ''', (
            product.get('name', None),
            product.get('price', 0.0),
            product.get('quantity', 0),
            product.get('category', 'Unknown'),
            product.get('fromCity', 'Unknown'),
            product.get('isAvailable', 1),
            product.get('views', 0)
        ))
    conn.commit()

def applying_changes(conn, cursor):
      for line in updates:
        try:
            name, method, param = line.strip().split('::')
            cursor.execute('SELECT * FROM products WHERE name = ?', (name,))
            product = cursor.fetchone()
            if not product:
                continue
            product_id, name, price, quantity, category, fromCity,\
            isAvailable, views, update_count = product
            if method == 'quantity_add':
                new_quantity = quantity + int(param)
                cursor.execute('UPDATE products SET quantity = ?, update_count = update_count + 1 WHERE id = ?',
                 (new_quantity, product_id))
            elif method == 'quantity_sub':
                new_quantity = quantity - int(param)
                if new_quantity >= 0:
                    cursor.execute('UPDATE products SET quantity = ?,update_count = update_count + 1 WHERE id = ?',
                     (new_quantity, product_id))
            elif method == 'price_percent':
                new_price = price * (1 + float(param))
                if new_price >= 0:
                    cursor.execute('UPDATE products SET price = ?, update_count = update_count + 1 WHERE id = ?',
                     (new_price, product_id))
            elif method == 'price_abs':
                new_price = price + float(param)
                if new_price >= 0:
                    cursor.execute('UPDATE products SET price = ?, update_count = update_count + 1 WHERE id = ?',
                     (new_price, product_id))
            elif method == 'available':
                cursor.execute('UPDATE products SET isAvailable = ?, update_count = update_count + 1 WHERE id = ?',
                 (int(param == 'True'), product_id))
            elif method == 'remove':
                cursor.execute('DELETE FROM products WHERE id = ?', (product_id,))
        except:
            continue
      conn.commit()


def analyze_products(conn):
    cursor = conn.cursor()
    queries = [
        (
            'Топ 10 наиболее обновляемых товаров',
            'SELECT name, update_count FROM products ORDER BY update_count DESC LIMIT 10'
        ),
        (
            'Анализ цен',
            '''
            SELECT 
                category,
                SUM(price) as total, 
                MIN(price) as min_price, 
                MAX(price) as max_price, 
                AVG(price) as avg_price, 
                COUNT(*) as product_count
            FROM products
            GROUP BY category
            '''
        ),
        (
            'Анализ остатков товаров по категориям в группах',
            '''
            SELECT 
                category, 
                SUM(quantity) as total_quantity, 
                MIN(quantity) as min_quantity, 
                MAX(quantity) as max_quantity, 
                AVG(quantity) as avg_quantity, 
                COUNT(*) as product_count
            FROM products
            GROUP BY category
            '''
        ),
        (
            'Топ 5 косметики с ценой <50',
            '''
            SELECT name, price 
            FROM products
            WHERE category = "cosmetics" AND price < 50
            ORDER BY price DESC LIMIT 5
            '''
        ),
    ]
    for title, query in queries:
        print(title)
        for row in cursor.execute(query):
            print(row)
        print()


if __name__ == "__main__":
    conn = sqlite3.connect('products.db')
    cursor = conn.cursor()
    create_table(conn, cursor)
    product_data, updates = load_data('_product_data.msgpack', '_update_data.text')
    if product_data:
        insert_data(conn, cursor, product_data)
    applying_changes(conn, cursor)
    analyze_products(conn)
    conn.close()
