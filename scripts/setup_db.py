import sqlite3

with open('src/main/resources/data/db.sql', 'rt') as f:
    sql = f.read()

try:
    connection = sqlite3.connect('de_test.db')
    cursor = connection.cursor()
    cursor.executescript(sql)
finally:
    cursor.close()
    connection.close()
