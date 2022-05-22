import sqlite3
from pathlib import Path

db_path = Path('de_test.db').absolute()

try:
    connection = sqlite3.connect(db_path)
    with open('src/main/resources/data/db.sql', 'wt') as f:
        for line in connection.iterdump():
            f.write('%s\n' % line)
finally:
    connection.close()
