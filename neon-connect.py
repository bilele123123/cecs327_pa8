import os
from psycopg2 import pool
from dotenv import load_dotenv

load_dotenv()

connection_string = os.getenv('DATABASE_URL')

connection_pool = pool.SimpleConnectionPool(
    1,
    10,
    connection_string
)

if connection_pool:
    print("Connection pool created successfully")

conn = connection_pool.getconn()

cur = conn.cursor()

cur.execute('SELECT NOW();')
time = cur.fetchone()[0]

cur.execute('SELECT version();')
version = cur.fetchone()[0]

cur.execute('SELECT * FROM fridge_data_metadata;')
metadata = cur.fetchone()

print(metadata)

cur.close()
connection_pool.putconn(conn)

connection_pool.closeall()

print('Current time:', time)
print('PostgreSQL version:', version)

